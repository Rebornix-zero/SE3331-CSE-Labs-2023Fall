#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {

void Worker::get_file_content(std::string filename, std::string &content) {
  // 获取文件inode ID
  auto res_lookup = chfs_client->lookup(1, filename);
  if (res_lookup.is_err()) {
    std::cout << "ERROR REQUEST1 " << filename << std::endl;
    res_lookup = chfs_client->lookup(1, filename);
  }
  auto inode_id = res_lookup.unwrap();
  // 获取文件大小
  auto res_type_attr = chfs_client->get_type_attr(inode_id);
  if (res_type_attr.is_err()) {
    std::cout << "ERROR REQUEST2" << std::endl;
  }
  auto type_attr = res_type_attr.unwrap();
  auto size = type_attr.second.size;
  // 读文件
  auto res_vec_content = chfs_client->read_file(inode_id, 0, size);
  if (res_vec_content.is_err()) {
    std::cout << "ERROR REQUEST3" << std::endl;
  }
  auto vec_content = res_vec_content.unwrap();
  // 为content赋值
  content.assign(vec_content.begin(), vec_content.end());
}

void Worker::write_file_content(
    std::string filename,
    std::vector<std::pair<std::string, std::string>> &result) {
  // 获取文件inode ID
  auto res_lookup = chfs_client->lookup(1, filename);
  if (res_lookup.is_err()) {
    std::cout << "ERROR REQUEST4" << filename << std::endl;
  }
  auto inode_id = res_lookup.unwrap();
  // 构建写入的string
  std::string content = "";
  for (auto &item : result) {
    content = content + item.first + " " + item.second + "\n";
  }

  // 构建写入的vector<u8>
  std::vector<chfs::u8> vec_content(content.begin(), content.end());
  // 写入
  chfs_client->write_file(inode_id, 0, vec_content);
}

Worker::Worker(MR_CoordinatorConfig config) {
  mr_client =
      std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
  outPutFile = config.resultFile;
  chfs_client = config.client;
  work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
  // Lab4: Your code goes here (Optional).
}

void Worker::doMap(std::vector<std::string> &files, std::string inter_file) {
  // Lab4: Your code goes here.
  std::vector<KeyVal> intermediate_data;
  std::vector<std::pair<std::string, std::string>> result;
  // 读取所有文件内容并map
  for (auto &file_name : files) {
    std::string content;
    get_file_content(file_name, content);
    auto map_result = Map(content);
    intermediate_data.insert(intermediate_data.end(), map_result.begin(),
                             map_result.end());
  }
  std::sort(intermediate_data.begin(), intermediate_data.end(),
            [](KeyVal const &a, KeyVal const &b) { return a.key < b.key; });

  // 先进行一次本地combine
  auto intermediate_size = intermediate_data.size();
  auto i = 0;
  while (i < intermediate_size) {
    std::string key = intermediate_data[i].key;
    std::vector<std::string> values;
    while (intermediate_data[i].key == key) {
      values.push_back(intermediate_data[i].val);
      ++i;
    }
    std::string reduce_result = Reduce(key, values);
    result.emplace_back(key, reduce_result);
  }
  // 向中间文件中写入数据
  write_file_content(inter_file, result);
}

std::vector<KeyVal> Worker::get_intermediate(std::string content) {
  std::vector<KeyVal> ret;
  auto length = content.size();
  for (unsigned int i = 0; i < length; ++i) {
    if (!isalpha(content[i])) {
      continue;
    }
    std::string word = "";
    std::string num = "";
    while (isalpha(content[i])) {
      word.push_back(content[i]);
      ++i;
    }
    while (!(isalpha(content[i])) &&
           !(content[i] >= '0' && content[i] <= '9')) {
      ++i;
    }

    while (content[i] >= '0' && content[i] <= '9') {
      num.push_back(content[i]);
      ++i;
    }

    ret.emplace_back(word, num);
  }
  return ret;
}

void Worker::doReduce(std::vector<std::string> &files) {
  // Lab4: Your code goes here.
  std::vector<KeyVal> intermediate_data;
  std::vector<std::pair<std::string, std::string>> result;
  // 读取所有中间文件内容并整合
  for (auto &file_name : files) {
    std::string content;
    get_file_content(file_name, content);
    auto map_result = get_intermediate(content);
    intermediate_data.insert(intermediate_data.end(), map_result.begin(),
                             map_result.end());
  }
  std::sort(intermediate_data.begin(), intermediate_data.end(),
            [](KeyVal const &a, KeyVal const &b) { return a.key < b.key; });

  // 全体reduce
  auto intermediate_size = intermediate_data.size();
  auto i = 0;
  while (i < intermediate_size) {
    std::string key = intermediate_data[i].key;
    std::vector<std::string> values;
    while (intermediate_data[i].key == key) {
      values.push_back(intermediate_data[i].val);
      ++i;
    }
    std::string reduce_result = Reduce(key, values);
    result.emplace_back(key, reduce_result);
  }

  // 写入输出文件中
  write_file_content(this->outPutFile, result);
}

void Worker::doSubmit(mr_tasktype taskType, int index, std::string inter_file) {
  // Lab4: Your code goes here.
  mr_client->call(SUBMIT_TASK, (int)(taskType), index, inter_file);
}

void Worker::stop() {
  shouldStop = true;
  work_thread->join();
}

void Worker::doWork() {
  while (!shouldStop) {
    // Lab4: Your code goes here.
    // 请求任务
    std::cout << "REQUEST" << std::endl;
    auto res_task = mr_client->call(ASK_TASK);
    if (res_task.is_err()) {
      std::cout << "ERROR REQUEST5" << std::endl;
    }
    // 解析三元组
    auto task =
        res_task.unwrap()
            ->as<std::tuple<int, int, std::vector<std::string>, std::string>>();
    int task_index = (std::get<0>(task));
    mr_tasktype task_type = (mr_tasktype)(std::get<1>(task));
    std::vector<std::string> files = (std::get<2>(task));
    std::string inter_file = (std::get<3>(task));
    switch (task_index) {
    case -2: {
      // 任务已完成，长时间休眠
      std::this_thread::sleep_for(std::chrono::milliseconds(2000));
      break;
    }
    case -1: {
      // 暂无任务
      break;
    }
    default: {
      if (task_type == mr_tasktype::MAP) {
        // std::cout << inter_file << std::endl;
        // map 任务处理
        this->doMap(files, inter_file);
        // map任务提交（需要附带创建的中间文件名字）
        doSubmit(task_type, task_index, inter_file);
      } else if (task_type == mr_tasktype::REDUCE) {
        // reduce 任务处理
        this->doReduce(files);
        // reduce任务提交
        doSubmit(task_type, task_index, inter_file);
      }
    }
    }
    // 休眠
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}
} // namespace mapReduce
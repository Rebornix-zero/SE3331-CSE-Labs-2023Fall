#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {

SequentialMapReduce::SequentialMapReduce(
    std::shared_ptr<chfs::ChfsClient> client,
    const std::vector<std::string> &files_, std::string resultFile) {
  // 执行mapReduce的主机
  chfs_client = std::move(client);
  // 需要读取数据的文件路径
  files = files_;
  // 输出结构的文件路径
  outPutFile = resultFile;
  // Your code goes here (optional)
}

// MY_MODIFY:

void SequentialMapReduce::get_file_content(std::string filename,
                                           std::string &content) {
  // 获取文件inode ID
  auto res_lookup = chfs_client->lookup(1, filename);
  auto inode_id = res_lookup.unwrap();
  // 获取文件大小
  auto res_type_attr = chfs_client->get_type_attr(inode_id);
  auto type_attr = res_type_attr.unwrap();
  auto size = type_attr.second.size;
  // 读文件
  auto res_vec_content = chfs_client->read_file(inode_id, 0, size);
  auto vec_content = res_vec_content.unwrap();
  // 为content赋值
  content.assign(vec_content.begin(), vec_content.end());
}

void SequentialMapReduce::write_file_content(
    std::vector<std::pair<std::string, std::string>> &result) {
  // 获取文件inode ID
  auto res_lookup = chfs_client->lookup(1, this->outPutFile);
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

void SequentialMapReduce::doWork() {
  // Your code goes here
  std::vector<KeyVal> intermediate_data;
  std::vector<std::pair<std::string, std::string>> result;

  // 读取所有文件，并将其内容Map得到中间结果
  for (auto &file_name : this->files) {
    std::string content;
    get_file_content(file_name, content);
    auto map_result = Map(content);
    intermediate_data.insert(intermediate_data.end(), map_result.begin(),
                             map_result.end());
  }

  // 中间结果sort以便同key的values聚合
  for (int i = 0; i < 10; ++i) {
    std::sort(intermediate_data.begin(), intermediate_data.end(),
              [](KeyVal const &a, KeyVal const &b) { return a.key < b.key; });
  }

  // 中间结果Reduce得到最终结果
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

  // 最终结果写入输出文件中
  write_file_content(result);
}
} // namespace mapReduce
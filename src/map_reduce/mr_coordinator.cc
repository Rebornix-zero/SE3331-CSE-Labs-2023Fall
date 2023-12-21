#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {

std::tuple<int, int, std::vector<std::string>, std::string>
Coordinator::askTask() {
  // Lab4 : Your code goes here.
  // Free to change the type of return value.
  // 接受请求后，将为worker选择一个任务，返回任务索引，任务类型，任务所需查阅的文件名,如果为map类型，还要分配新的中间数据文件名
  // 如果返回的任务索引为-1,即代表无任务需要分配
  // 如果返回的任务索引为-2,则代表全部任务已经完成，worker应该休眠
  std::unique_lock<std::mutex> uniqueLock(this->mtx);

  if (this->isFinished) {
    uniqueLock.unlock();
    return std::make_tuple(-2, -1, std::vector<std::string>(), "");
  }
  if (map_finish_num < map_task_list.size()) {
    // 分配map task
    for (int i = 0; i < map_task_list.size(); ++i) {
      if (map_task_list[i].state == taskState::WAIT) {
        map_task_list[i].state = taskState::EXECUTE;
        // 申请新的中间文件
        std::string inter_file = "inter_" + map_task_list[i].src_file[0];
        chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, inter_file);
        uniqueLock.unlock();
        return std::make_tuple(i, (int)(mr_tasktype::MAP),
                               map_task_list[i].src_file, inter_file);
      }
    }
  } else {
    // 分配reduce task
    for (int i = 0; i < reduce_task_list.size(); ++i) {
      if (reduce_task_list[i].state == taskState::WAIT) {
        reduce_task_list[i].state = taskState::EXECUTE;

        uniqueLock.unlock();
        return std::make_tuple(i, (int)(mr_tasktype::REDUCE),
                               reduce_task_list[i].src_file_list, "");
      }
    }
  }
  // 无任务分配
  uniqueLock.unlock();
  return std::make_tuple(-1, -1, std::vector<std::string>(), "");
}

int Coordinator::submitTask(int taskType, int index, std::string inter_file) {
  // Lab4 : Your code goes here.
  std::unique_lock<std::mutex> uniqueLock(this->mtx);
  if ((mr_tasktype)(taskType) == mr_tasktype::MAP) {
    // map 任务完成
    // 修改对应任务状态
    map_task_list[index].state = taskState::FINFISH;
    // 记录中间数据文件
    intermediate_file_list.push_back(inter_file);
    // 完成任务数+1,如果全部完成则为reduce进行任务创建
    ++map_finish_num;
    if (map_finish_num == map_task_list.size()) {
      reduce_task_list.emplace_back(intermediate_file_list);
      reduce_finish_num = 0;
    }
  } else {
    // reduce 任务完成
    // 修改对应任务状态
    reduce_task_list[index].state = taskState::FINFISH;
    // 完成任务数+1,如果全部完成则标记整个任务完成
    ++reduce_finish_num;
    if (reduce_finish_num == reduce_task_list.size()) {
      this->isFinished = true;
    }
  }

  uniqueLock.unlock();
  return 0;
}

// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
bool Coordinator::Done() {
  std::unique_lock<std::mutex> uniqueLock(this->mtx);
  return this->isFinished;
}

// create a Coordinator.
// nReduce is the number of reduce tasks to use.
Coordinator::Coordinator(MR_CoordinatorConfig config,
                         const std::vector<std::string> &files, int nReduce) {
  this->files = files;
  this->isFinished = false;
  // Lab4: Your code goes here (Optional).

  this->map_finish_num = 0;
  this->reduce_finish_num = 0;
  this->worker_num = nReduce;
  // 根据files内容，对每个input文件创建一个map task
  map_task_list.emplace_back(files);
  chfs_client = config.client;

  rpc_server =
      std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
  rpc_server->bind(ASK_TASK, [this]() { return this->askTask(); });
  rpc_server->bind(SUBMIT_TASK,
                   [this](int taskType, int index, std::string inter_file) {
                     return this->submitTask(taskType, index, inter_file);
                   });
  rpc_server->run(true, 1);
}
} // namespace mapReduce
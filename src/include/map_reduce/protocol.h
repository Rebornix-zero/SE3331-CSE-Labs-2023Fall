#include "distributed/client.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include <mutex>
#include <string>
#include <utility>
#include <vector>

// Lab4: Free to modify this file

namespace mapReduce {

// 键值对结构，均为string类型
struct KeyVal {
  KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
  KeyVal() {}
  std::string key;
  std::string val;
};

// 工作类型
enum mr_tasktype { NONE = 0, MAP, REDUCE };

// Word Count的Mapper工作逻辑
std::vector<KeyVal> Map(const std::string &content);
// Word Count的Reducer工作逻辑
std::string Reduce(const std::string &key,
                   const std::vector<std::string> &values);

const std::string ASK_TASK = "ask_task";
const std::string SUBMIT_TASK = "submit_task";

struct MR_CoordinatorConfig {
  uint16_t port;
  std::string ip_address;
  std::string resultFile;
  std::shared_ptr<chfs::ChfsClient> client;

  MR_CoordinatorConfig(std::string ip_address, uint16_t port,
                       std::shared_ptr<chfs::ChfsClient> client,
                       std::string resultFile)
      : port(port), ip_address(std::move(ip_address)), resultFile(resultFile),
        client(std::move(client)) {}
};

class SequentialMapReduce {
public:
  SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                      const std::vector<std::string> &files,
                      std::string resultFile);
  void doWork();
  void get_file_content(std::string filename, std::string &content);
  void
  write_file_content(std::vector<std::pair<std::string, std::string>> &result);

private:
  std::shared_ptr<chfs::ChfsClient> chfs_client;
  std::vector<std::string> files;
  std::string outPutFile;
};

class Coordinator {
public:
  Coordinator(MR_CoordinatorConfig config,
              const std::vector<std::string> &files, int nReduce);
  std::tuple<int, int, std::vector<std::string>, std::string> askTask();
  int submitTask(int taskType, int index, std::string inter_file);
  bool Done();

private:
  enum taskState { WAIT, EXECUTE, FINFISH };

  struct MapTask {
    std::string src_file;
    taskState state;
    MapTask(std::string file) {
      this->src_file = file;
      this->state = taskState::WAIT;
    }
  };

  struct ReduceTask {
    std::vector<std::string> src_file_list;
    taskState state;
    ReduceTask(std::vector<std::string> file_list) {
      this->src_file_list = file_list;
      this->state = taskState::WAIT;
    }
  };

  std::vector<std::string> files;
  std::mutex mtx;
  std::unique_ptr<chfs::RpcServer> rpc_server;
  // 记录是否完成了整个任务
  bool isFinished;
  // 记录map task完成的个数，如果全部完成，可以执行reduce
  int map_finish_num;
  // 记录reduce task完成的个数，如果全部完成，可以执行reduce
  int reduce_finish_num;
  // 记录worker总数
  int worker_num;
  // map task列表
  std::vector<MapTask> map_task_list;
  // reduce task 列表
  std::vector<ReduceTask> reduce_task_list;
  // 中间数据文件存放位置
  std::vector<std::string> intermediate_file_list;
  std::shared_ptr<chfs::ChfsClient> chfs_client;
};

class Worker {
public:
  explicit Worker(MR_CoordinatorConfig config);
  void doWork();
  void stop();

private:
  void doMap(std::vector<std::string> &files, std::string inter_file);
  void doReduce(std::vector<std::string> &files);
  void doSubmit(mr_tasktype taskType, int index, std::string inter_file);

  std::string outPutFile;
  std::unique_ptr<chfs::RpcClient> mr_client;
  std::shared_ptr<chfs::ChfsClient> chfs_client;
  std::unique_ptr<std::thread> work_thread;
  bool shouldStop = false;

  void get_file_content(std::string filename, std::string &content);
  void
  write_file_content(std::string filename,
                     std::vector<std::pair<std::string, std::string>> &result);
  std::vector<KeyVal> get_intermediate(std::string content);
};

} // namespace mapReduce

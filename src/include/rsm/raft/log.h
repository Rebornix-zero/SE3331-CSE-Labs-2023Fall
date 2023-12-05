#pragma once

#include "block/manager.h"
#include "common/macros.h"
#include <cstring>
#include <mutex>
#include <vector>

namespace chfs {

// MY_MODIFY: start
template <typename content_type> struct LogEntry {
  int term = 0;
  int logic_index = 0;
  content_type content;
};
// MY_MODIFY: end

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command> class RaftLog {
public:
  RaftLog(std::shared_ptr<BlockManager> bm);
  ~RaftLog();
  /* Lab3: Your code here */
  // MY_MODIFY:
  bool persist_metadata(int current_term, int leader_id);
  bool persist_log(std::vector<LogEntry<Command>> &log_list);
  bool persist_snapshot();

private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  /* Lab3: Your code here */
  std::vector<LogEntry<Command>>
      *log_list_ptr; // 从外部传入的指针，指向实际存储的log list
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) {
  /* Lab3: Your code here */
}

template <typename Command> RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}

/* Lab3: Your code here */

} /* namespace chfs */

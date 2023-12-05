#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdarg.h>
#include <thread>
#include <unistd.h>

#include "block/manager.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"

// MY_MODIFY:
#include <random>

namespace chfs {

enum class RaftRole { Follower, Candidate, Leader };

struct RaftNodeConfig {
  int node_id;
  uint16_t port;
  std::string ip_address;
};

template <typename StateMachine, typename Command> class RaftNode {

#define RAFT_LOG(fmt, args...)                                                 \
  do {                                                                         \
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(          \
                   std::chrono::system_clock::now().time_since_epoch())        \
                   .count();                                                   \
    char buf[512];                                                             \
    sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now,       \
            __FILE__, __LINE__, my_id, current_term, role, ##args);            \
    thread_pool->enqueue([=]() { std::cerr << buf; });                         \
  } while (0);

public:
  RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
  ~RaftNode();

  /* interfaces for test */
  void set_network(std::map<int, bool> &network_availablility);
  void set_reliable(bool flag);
  int get_list_state_log_num();
  int rpc_count();
  std::vector<u8> get_snapshot_direct();

private:
  /*
   * Start the raft node.
   * Please make sure all of the rpc request handlers have been registered
   * before this method.
   */
  auto start() -> int;

  /*
   * Stop the raft node.
   */
  auto stop() -> int;

  /* Returns whether this node is the leader, you should also return the current
   * term. */
  auto is_leader() -> std::tuple<bool, int>;

  /* Checks whether the node is stopped */
  auto is_stopped() -> bool;

  /*
   * Send a new command to the raft nodes.
   * The returned tuple of the method contains three values:
   * 1. bool:  True if this raft node is the leader that successfully appends
   * the log, false If this node is not the leader.
   * 2. int: Current term.
   * 3. int: Log index.
   */
  auto new_command(std::vector<u8> cmd_data, int cmd_size)
      -> std::tuple<bool, int, int>;

  /* Save a snapshot of the state machine and compact the log. */
  auto save_snapshot() -> bool;

  /* Get a snapshot of the state machine */
  auto get_snapshot() -> std::vector<u8>;

  /* Internal RPC handlers */
  auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
  auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
  auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

  /* RPC helpers */
  void send_request_vote(int target, RequestVoteArgs arg);
  void handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                 const RequestVoteReply reply);

  void send_append_entries(int target, AppendEntriesArgs<Command> arg);
  void handle_append_entries_reply(int target,
                                   const AppendEntriesArgs<Command> arg,
                                   const AppendEntriesReply reply);

  void send_install_snapshot(int target, InstallSnapshotArgs arg);
  void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg,
                                     const InstallSnapshotReply reply);

  /* background workers */
  void run_background_ping();
  void run_background_election();
  void run_background_commit();
  void run_background_apply();

  /* Data structures */
  bool network_stat; /* for test */

  std::mutex mtx;         /* A big lock to protect the whole data structure. */
  std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
  std::unique_ptr<ThreadPool> thread_pool;
  std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
  std::unique_ptr<StateMachine> state; /*  The state machine that applies the
                                          raft log, e.g. a kv store. */

  std::unique_ptr<RpcServer>
      rpc_server; /* RPC server to recieve and handle the RPC requests. */
  std::map<int, std::unique_ptr<RpcClient>>
      rpc_clients_map; /* RPC clients of all raft nodes including this node. */
  std::vector<RaftNodeConfig> node_configs; /* Configuration for all nodes */
  int my_id; /* The index of this node in rpc_clients, start from 0. */

  std::atomic_bool stopped;

  RaftRole role;
  int current_term;
  int leader_id; // 代表当前任期下投票的对象

  std::unique_ptr<std::thread> background_election;
  std::unique_ptr<std::thread> background_ping;
  std::unique_ptr<std::thread> background_commit;
  std::unique_ptr<std::thread> background_apply;

  /* Lab3: Your code here */
  // MY_MODIFY: data
  std::vector<LogEntry<Command>>
      log_list;       // raft_log的内存中内容(第0个是头节点)
  int agree_num;      // 记录候选状态下的候选任期和得票数量
  int max_commit_idx; // 记录提交的log entry的最大index
  int max_apply_idx;  // 记录成功apply的log entry的最大index
  std::vector<int> next_index; // leader记录的向其他follower添加的下一log的index
  std::vector<int>
      match_index; // leader记录的每个follower对log的同步程度，用于更新commit_index
  unsigned long
      last_received_heartbeat_RPC_time; // 记录上次接受到heartbeat RPC的时间
  unsigned long election_start_time; // 记录本轮选举开始的时间
  unsigned long
      time_out_election; // candidate因为选举超时而开始下一term的时间限制，会随机化
  unsigned long
      time_out_heartbeat; // follower因为heartbeat超时而开始下一term的时间限制，会随机化
  std::shared_ptr<BlockManager> bm;

  // MY_MODIFY: fun
  auto get_last_log_term() -> int;
  auto get_last_log_index() -> int;
  auto get_time() -> unsigned long;
  auto restart_random_time_out(int max, int min) -> unsigned long;
  auto renew_heartbeat_time() -> void;
  auto renew_start_election_time() -> void;
  auto index_logic2phy(int logic) -> int;
  auto index_phy2logic(int phy) -> int;
  auto get_clients_num() -> int;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id,
                                          std::vector<RaftNodeConfig> configs)
    : network_stat(true), node_configs(configs), my_id(node_id), stopped(true),
      role(RaftRole::Follower), current_term(0), leader_id(-1) {
  auto my_config = node_configs[my_id];

  /* launch RPC server */
  rpc_server =
      std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

  /* Register the RPCs. */
  rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
  rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
  rpc_server->bind(RAFT_RPC_CHECK_LEADER,
                   [this]() { return this->is_leader(); });
  rpc_server->bind(RAFT_RPC_IS_STOPPED,
                   [this]() { return this->is_stopped(); });
  rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                   [this](std::vector<u8> data, int cmd_size) {
                     return this->new_command(data, cmd_size);
                   });
  rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT,
                   [this]() { return this->save_snapshot(); });
  rpc_server->bind(RAFT_RPC_GET_SNAPSHOT,
                   [this]() { return this->get_snapshot(); });

  rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) {
    return this->request_vote(arg);
  });
  rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) {
    return this->append_entries(arg);
  });
  rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) {
    return this->install_snapshot(arg);
  });

  /* Lab3: Your code here */
  // TODO: finish initialize
  this->bm = std::make_shared<BlockManager>("/tmp/raft_log/data" +
                                            std::to_string(this->my_id));
  this->thread_pool = std::make_unique<ThreadPool>(32);
  this->log_storage = std::make_unique<RaftLog<Command>>(this->bm);
  this->state = std::make_unique<StateMachine>();

  // 头节点推入
  LogEntry<Command> obj;
  obj.logic_index = 0;
  obj.term = 0;
  this->log_list.push_back(obj);

  this->agree_num = 0;
  this->max_commit_idx = 0;
  this->max_apply_idx = 0;
  this->next_index.resize(this->node_configs.size());
  this->match_index.resize(this->node_configs.size());
  this->last_received_heartbeat_RPC_time = this->get_time();
  this->election_start_time = this->get_time();
  this->time_out_election = this->restart_random_time_out(300, 150);
  this->time_out_heartbeat = this->restart_random_time_out(300, 150);

  rpc_server->run(true, configs.size());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode() {
  stop();

  thread_pool.reset();
  rpc_server.reset();
  state.reset();
  log_storage.reset();

  /* Lab3: Your code here */
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/

// MY_MODIFY: fun imp start
template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_last_log_term() -> int {
  int size = this->log_list.size();
  if (size == 0)
    return 0;
  LogEntry<Command> last_log_entry = log_list[size - 1];
  return last_log_entry.term;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_last_log_index() -> int {
  int size = this->log_list.size();
  if (size == 0)
    return 0;
  LogEntry<Command> last_log_entry = log_list[size - 1];
  return last_log_entry.logic_index;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_time() -> unsigned long {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::restart_random_time_out(int max, int min)
    -> unsigned long {
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine generator(seed);
  std::uniform_int_distribution<unsigned long> distribution(min, max);
  int result = distribution(generator);
  return result;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::renew_heartbeat_time() -> void {
  this->last_received_heartbeat_RPC_time = this->get_time();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::renew_start_election_time() -> void {
  this->election_start_time = this->get_time();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::index_logic2phy(int logic) -> int {
  if (logic < 0) {
    return -1;
  }
  return logic;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::index_phy2logic(int phy) -> int {
  if (phy >= 0 && phy < this->log_list.size()) {
    return (this->log_list)[phy].logic_index;
  }
  return -1;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_clients_num() -> int {
  return this->rpc_clients_map.size();
}
// MY_MODIFY: fun imp end

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int {
  /* Lab3: Your code here */
  // 初始化rpc map
  this->rpc_clients_map.clear();
  for (auto &item : this->node_configs) {
    this->rpc_clients_map.insert(std::make_pair(
        item.node_id,
        std::make_unique<RpcClient>(item.ip_address, item.port, true)));
  }
  // 启动标志更改
  this->stopped.store(false);
  // 启动线程创建
  background_election =
      std::make_unique<std::thread>(&RaftNode::run_background_election, this);
  background_ping =
      std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
  background_commit =
      std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
  background_apply =
      std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
  /* Lab3: Your code here */
  this->stopped.store(true);
  this->background_election->join();
  this->background_ping->join();
  this->background_commit->join();
  this->background_apply->join();
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
  /* Lab3: Your code here */
  std::lock_guard<std::mutex> lock(this->mtx);
  return std::make_tuple(this->role == RaftRole::Leader, this->current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
  return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data,
                                                  int cmd_size)
    -> std::tuple<bool, int, int> {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(this->mtx);
  // 检查节点身份，只有leader可以接受用户的新log并添加
  if (this->role == RaftRole::Leader) {
    int next_log_index = this->get_last_log_index() + 1;
    Command cmd;
    cmd.deserialize(cmd_data, cmd_size);

    LogEntry<Command> tmp;
    tmp.content = cmd;
    tmp.term = this->current_term;
    tmp.logic_index = next_log_index;
    this->log_list.push_back(tmp);
    lock.unlock();
    RAFT_LOG("Leader add log %d", next_log_index);
    return std::make_tuple(true, this->current_term, next_log_index);
  } else {
    lock.unlock();
    return std::make_tuple(false, this->current_term,
                           this->get_last_log_index());
  }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
  /* Lab3: Your code here */
  return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
  /* Lab3: Your code here */
  return std::vector<u8>();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args)
    -> RequestVoteReply {
  RAFT_LOG("request_vote");
  /* Lab3: Your code here */
  RequestVoteReply reply;
  std::unique_lock<std::mutex> lock(this->mtx);
  // 查看是否更新本地任期与卸任
  if (args.candidate_term > this->current_term) {
    this->current_term = args.candidate_term;
    this->leader_id = -1;
    this->role = RaftRole::Follower;
    this->agree_num = 0;
    this->time_out_heartbeat = this->restart_random_time_out(300, 150);
    this->renew_heartbeat_time();
  }
  reply.vote_id = this->my_id;
  reply.vote_term = this->current_term;

  // 查看是否可以投票
  bool term_bo = (this->current_term == args.candidate_term);
  bool votefor_bo =
      (this->leader_id == args.candidate_id) || (this->leader_id == -1);
  bool log_complete_bo =
      !((this->get_last_log_term() > args.last_log_term) ||
        ((this->get_last_log_term() == args.last_log_term) &&
         (this->get_last_log_index() > args.last_log_index)));
  if (term_bo && votefor_bo && log_complete_bo) {
    // 可以投票
    // 更新心跳时间
    RAFT_LOG("vote to %d", args.candidate_id);
    this->renew_heartbeat_time();
    reply.vote_granted = true;
    this->leader_id = args.candidate_id;
    this->role = RaftRole::Follower;
  } else {
    // 拒绝投票
    reply.vote_granted = false;
  }
  lock.unlock();
  return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(
    int target, const RequestVoteArgs arg, const RequestVoteReply reply) {
  /* Lab3: Your code here */
  RAFT_LOG("handle_request_vote_reply");
  std::unique_lock<std::mutex> lock(this->mtx);
  // 检查是否任期过期，是否卸任
  if (reply.vote_term > this->current_term) {
    // 更新当前时期，并卸任
    this->current_term = reply.vote_term;
    this->leader_id = -1;
    this->role = RaftRole::Follower;
    this->agree_num = 0;
    this->time_out_heartbeat = this->restart_random_time_out(300, 150);
    this->renew_heartbeat_time();
  }
  // 检查reply是否投票
  if (reply.vote_granted) {
    // 同意投票，若满足多数则上任leader
    this->agree_num += 1;
    if (this->role == RaftRole::Candidate &&
        this->agree_num >= (this->rpc_clients_map.size() / 2 + 1)) {
      // 获得半数以上选票，成为当期leader
      this->role = RaftRole::Leader;
      RAFT_LOG("Become Leader");
      int init_next_index = this->get_last_log_index() + 1;
      for (int i = 0; i < this->rpc_clients_map.size(); ++i) {
        this->next_index[i] = init_next_index;
        this->match_index[i] = 0;
      }
    } else {
      // RAFT_LOG("Already Leader");
    }
  } else {
    // 拒绝投票
  }

  lock.unlock();
  return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(
    RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
  /* Lab3: Your code here */
  RAFT_LOG("append_entries");
  AppendEntriesReply result;
  result.success = false;
  AppendEntriesArgs<Command> arg =
      transform_rpc_append_entries_args<Command>(rpc_arg);
  std::unique_lock<std::mutex> lock(this->mtx);
  // leader,follower检测是否有大于等于的term,如果是，则卸任/更改leader
  if (arg.leader_term >= this->current_term) {
    this->current_term = arg.leader_term;
    this->role = RaftRole::Follower;
    this->agree_num = 0;
    this->leader_id = arg.leader_id;
    this->time_out_heartbeat = this->restart_random_time_out(300, 150);
    this->renew_heartbeat_time();
  }
  // 检测该请求的资格是否足够，若足够则可以监测心跳和复制log
  if (this->current_term == arg.leader_term) {
    int last_new_entry_index;
    // 更新接受心跳rpc的时间
    this->renew_heartbeat_time();
    // 检测是否为心跳
    if (arg.entries.empty()) {
      // 是心跳
      // 检测pre log 情况
      int phy_log_index = this->index_logic2phy(arg.prev_log_index);
      if (phy_log_index == -1 || phy_log_index >= this->log_list.size() ||
          (this->log_list)[phy_log_index].term != arg.prev_log_term) {
        // rpc逻辑地址不合法，物理地址越界，物理地址对应的log
        // term不一致，拒绝(情况2)
        lock.unlock();
        result.term = current_term;
        return result;
      } else {
        result.success = true;
        last_new_entry_index = this->log_list[phy_log_index].logic_index;
      }
    } else {
      // 不是心跳，需要复制log
      int phy_log_index = this->index_logic2phy(arg.prev_log_index);
      if (phy_log_index == -1 || phy_log_index >= this->log_list.size() ||
          (this->log_list)[phy_log_index].term != arg.prev_log_term) {
        // rpc逻辑地址不合法，物理地址越界，物理地址对应的log
        // term不一致，拒绝(情况2)
        lock.unlock();
        result.term = current_term;
        return result;
      }
      result.success = true;
      last_new_entry_index = this->log_list[phy_log_index].logic_index;
      // 复制log
      phy_log_index += 1;
      for (int i = 0; i < arg.entries.size(); ++i) {
        if (phy_log_index + i >= this->log_list.size()) {
          // 越界了，push
          // 更新index of last new entry
          ++last_new_entry_index;
          LogEntry<Command> obj;
          obj.term = arg.leader_term;
          obj.logic_index = last_new_entry_index;
          obj.content = arg.entries[i];
          this->log_list.push_back(obj);
        } else {
          // 不越界，modify
          this->log_list[i + phy_log_index].term = arg.leader_term;
          this->log_list[i + phy_log_index].content = arg.entries[i];
          // 更新index of last new entry
          ++last_new_entry_index;
        }
      }
    }
    // 更新本机commit_idx
    if (arg.leader_commit > this->max_commit_idx) {
      this->max_commit_idx = std::min(arg.leader_commit, last_new_entry_index);
    }
  }
  // 准备返回
  lock.unlock();
  result.term = current_term;
  return result;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(
    int node_id, const AppendEntriesArgs<Command> arg,
    const AppendEntriesReply reply) {
  RAFT_LOG("handle_append_entries_reply");
  std::unique_lock<std::mutex> lock(this->mtx);
  // 更新term信息，并选择是否卸任
  if (reply.term > this->current_term) {
    this->current_term = reply.term;
    this->role = RaftRole::Follower;
    this->leader_id = -1;
    this->time_out_heartbeat = this->restart_random_time_out(300, 150);
    this->renew_heartbeat_time();
  }
  // 若更新后不为leader，则无需处理reply
  if (this->role != RaftRole::Leader) {
    lock.unlock();
    return;
  }
  if (reply.success) {
    // 成功
    // 若推入的entries不为空，则更新相关内容
    if (!arg.entries.empty()) {
      // 更新next_index，match_index表
      this->match_index[node_id] = arg.entries.size() + arg.prev_log_index;
      this->next_index[node_id] = this->match_index[node_id] + 1;
      RAFT_LOG("follower id:%d, prev index %d, entries num %d, match index %d, "
               "next index %d",
               node_id, arg.prev_log_index, (int)arg.entries.size(),
               this->match_index[node_id], this->next_index[node_id]);
      // 更新commit_index
      for (int i = this->index_logic2phy(this->max_commit_idx + 1);
           i < this->log_list.size(); ++i) {
        int cnt = 0;
        // 非本term的log不做commit检查
        if (this->log_list[i].term != this->current_term)
          continue;
        // 计数
        int logic_log_index = this->index_phy2logic(i);
        for (int j = 0; j < this->get_clients_num(); ++j) {
          if (j == this->my_id) {
            ++cnt;
            continue;
          }
          if (match_index[j] >= logic_log_index)
            ++cnt;
        }
        // 检查该log是否可以commit
        if (cnt >= (this->get_clients_num() / 2) + 1) {
          // 可以更新commit_idx
          this->max_commit_idx = this->index_logic2phy(i);
        }
      }
    }
  } else {
    // 失败
    this->next_index[node_id] -= 1;
  }
  lock.unlock();
  return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args)
    -> InstallSnapshotReply {
  /* Lab3: Your code here */
  return InstallSnapshotReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(
    int node_id, const InstallSnapshotArgs arg,
    const InstallSnapshotReply reply) {
  /* Lab3: Your code here */
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id,
                                                        RequestVoteArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_request_vote_reply(target_id, arg,
                              res.unwrap()->as<RequestVoteReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(
    int target_id, AppendEntriesArgs<Command> arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_append_entries_reply(target_id, arg,
                                res.unwrap()->as<AppendEntriesReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(
    int target_id, InstallSnapshotArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_install_snapshot_reply(target_id, arg,
                                  res.unwrap()->as<InstallSnapshotReply>());
  } else {
    // RPC fails
  }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
  // Periodly check the liveness of the leader.

  // Work for followers and candidates.

  /* Uncomment following code when you finish */
  // while (true) {
  //     {
  //         if (is_stopped()) {
  //             return;
  //         }
  //         /* Lab3: Your code here */
  //     }
  // }
  while (true) {
    {
      if (this->is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(this->mtx);
      // 检查是否超时，如果超时则成为下一term的candidate
      bool follow_over =
          ((this->role == RaftRole::Follower) &&
           ((this->get_time() - this->last_received_heartbeat_RPC_time) >
            this->time_out_heartbeat));
      bool candidate_over = ((this->role == RaftRole::Candidate) &&
                             ((this->get_time() - this->election_start_time) >
                              this->time_out_election));
      if (follow_over || candidate_over) {
        // 超时，成为下一任期的候选人
        this->role = RaftRole::Candidate;
        this->current_term += 1;
        this->agree_num = 1; // 投自己一票
        this->leader_id = this->my_id;
        RAFT_LOG("Become candidate");
        this->renew_start_election_time();
        this->time_out_election = this->restart_random_time_out(300, 150);
        for (int i = 0; i < this->rpc_clients_map.size(); ++i) {
          if (i == this->my_id)
            continue;
          RequestVoteArgs request_vote_arg;
          request_vote_arg.candidate_id = this->my_id;
          request_vote_arg.candidate_term = this->current_term;
          request_vote_arg.last_log_index = this->get_last_log_index();
          request_vote_arg.last_log_term = this->get_last_log_term();
          this->thread_pool->enqueue(&RaftNode::send_request_vote, this, i,
                                     request_vote_arg);
        }
      }
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
  // Periodly send logs to the follower.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(this->mtx);
      // 只有leader有权限向follower复制log
      if (this->role == RaftRole::Leader) {
        for (int i = 0; i < this->get_clients_num(); ++i) {
          if (i == this->my_id)
            continue;
          // 如果无新的entry,就不发了
          AppendEntriesArgs<Command> tmp;
          // 将pre log index的后续所有log装入
          int pre_index_raw = this->index_logic2phy(this->next_index[i] - 1);
          for (int j = pre_index_raw + 1; j < this->log_list.size(); ++j) {
            tmp.entries.push_back(this->log_list[j].content);
          }
          if (tmp.entries.empty())
            continue;
          tmp.leader_term = this->current_term;
          tmp.leader_id = this->my_id;
          tmp.prev_log_index = this->next_index[i] - 1;
          tmp.prev_log_term = (this->log_list)[pre_index_raw].term;
          tmp.leader_commit = this->max_commit_idx;
          this->thread_pool->enqueue(&RaftNode::send_append_entries, this, i,
                                     tmp);
        }
      }
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(40));
    }
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
  // Periodly apply committed logs the state machine

  // Work for all the nodes.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(this->mtx);
      for (int phy_log_idx = this->index_logic2phy(this->max_apply_idx + 1),
               i = this->max_apply_idx + 1;
           i <= this->max_commit_idx; ++phy_log_idx, ++i) {
        // 检测物理地址合法
        if (phy_log_idx >= 1 && phy_log_idx < this->log_list.size()) {
          // 合法，apply log
          this->state->apply_log(this->log_list[phy_log_idx].content);
          // 更新apply idx
          ++(this->max_apply_idx);
        } else {
          // 不合法，报错，退出
          RAFT_LOG("Invalid in apply");
          break;
        }
      }
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
  // Periodly send empty append_entries RPC to the followers.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  // while (true) {
  //     {
  //         if (is_stopped()) {
  //             return;
  //         }
  //         /* Lab3: Your code here */
  //     }
  // }
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(this->mtx);
      // 只有leader可以ping
      if (this->role == RaftRole::Leader) {
        for (int i = 0; i < this->get_clients_num(); ++i) {
          if (i == this->my_id)
            continue;
          AppendEntriesArgs<Command> tmp;
          tmp.leader_term = this->current_term;
          tmp.leader_id = this->my_id;
          tmp.prev_log_index = this->next_index[i] - 1;
          tmp.prev_log_term =
              (this->log_list)[this->index_logic2phy(this->next_index[i] - 1)]
                  .term;
          tmp.leader_commit = this->max_commit_idx;
          this->thread_pool->enqueue(&RaftNode::send_append_entries, this, i,
                                     tmp);
        }
      }
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
  return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(
    std::map<int, bool> &network_availability) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  /* turn off network */
  if (!network_availability[my_id]) {
    for (auto &&client : rpc_clients_map) {
      if (client.second != nullptr)
        client.second.reset();
    }

    return;
  }

  for (auto node_network : network_availability) {
    int node_id = node_network.first;
    bool node_status = node_network.second;

    if (node_status && rpc_clients_map[node_id] == nullptr) {
      RaftNodeConfig target_config;
      for (auto config : node_configs) {
        if (config.node_id == node_id)
          target_config = config;
      }

      rpc_clients_map[node_id] = std::make_unique<RpcClient>(
          target_config.ip_address, target_config.port, true);
    }

    if (!node_status && rpc_clients_map[node_id] != nullptr) {
      rpc_clients_map[node_id].reset();
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      client.second->set_reliable(flag);
    }
  }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num() {
  /* only applied to ListStateMachine*/
  std::unique_lock<std::mutex> lock(mtx);

  return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count() {
  int sum = 0;
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      sum += client.second->count();
    }
  }

  return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
  if (is_stopped()) {
    return std::vector<u8>();
  }

  std::unique_lock<std::mutex> lock(mtx);

  return state->snapshot();
}

} // namespace chfs
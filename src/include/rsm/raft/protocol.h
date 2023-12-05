#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"
#include <vector>

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
  /* Lab3: Your code here */
  int candidate_id;
  int candidate_term;
  int last_log_index;
  int last_log_term;
  MSGPACK_DEFINE(candidate_id, candidate_term, last_log_index, last_log_term)
};

struct RequestVoteReply {
  /* Lab3: Your code here */
  int vote_id;
  int vote_term;
  bool vote_granted;
  MSGPACK_DEFINE(vote_id, vote_term, vote_granted)
};

template <typename Command> struct AppendEntriesArgs {
  /* Lab3: Your code here */
  int leader_term;
  int leader_id;
  int prev_log_index;
  int prev_log_term;
  int leader_commit;
  std::vector<Command> entries;
};

struct RpcAppendEntriesArgs {
  /* Lab3: Your code here */
  int leader_term;
  int leader_id;
  int prev_log_index;
  int prev_log_term;
  int leader_commit;
  std::vector<u8> entries;
  MSGPACK_DEFINE(leader_term, leader_id, prev_log_index, prev_log_term,
                 leader_commit, entries)
};

template <typename Command>
RpcAppendEntriesArgs
transform_append_entries_args(const AppendEntriesArgs<Command> &arg) {
  /* Lab3: Your code here */
  RpcAppendEntriesArgs result;
  result.leader_term = arg.leader_term;
  result.leader_id = arg.leader_id;
  result.prev_log_index = arg.prev_log_index;
  result.prev_log_term = arg.prev_log_term;
  result.leader_commit = arg.leader_commit;
  for (Command item : arg.entries) {
    std::vector<u8> ser_item = item.serialize(item.size());
    for (int i = 0; i < ser_item.size(); ++i)
      result.entries.push_back(ser_item[i]);
  }
  return result;
}

template <typename Command>
AppendEntriesArgs<Command>
transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg) {
  /* Lab3: Your code here */
  AppendEntriesArgs<Command> result;
  result.leader_term = rpc_arg.leader_term;
  result.leader_id = rpc_arg.leader_id;
  result.prev_log_index = rpc_arg.prev_log_index;
  result.prev_log_term = rpc_arg.prev_log_term;
  result.leader_commit = rpc_arg.leader_commit;
  Command obj;
  std::vector<u8> data;
  int current_length = 0;
  int require_length = obj.size();
  for (u8 char_item : rpc_arg.entries) {
    data.push_back(char_item);
    ++current_length;
    if (current_length == require_length) {
      Command tmp;
      tmp.deserialize(data, require_length);
      result.entries.push_back(tmp);
      current_length = 0;
      data.clear();
    }
  }
  return result;
}

struct AppendEntriesReply {
  /* Lab3: Your code here */
  int term;
  int success;
  MSGPACK_DEFINE(term, success)
};

struct InstallSnapshotArgs {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
};

struct InstallSnapshotReply {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
};

} /* namespace chfs */
#pragma once

#include "block/manager.h"
#include "common/macros.h"
#include <cstring>
#include <mutex>
#include <vector>

namespace chfs {

// MY_MODIFY:
template <typename content_type> struct LogEntry {
  int term = 0;
  int logic_index = 0;
  content_type content;

  int serialize_size() {
    return sizeof(term) + sizeof(logic_index) + content.size();
  }

  void serialize(std::vector<u8> &data) {
    if (!data.empty()) {
      return;
    }
    data.resize(this->serialize_size());
    u8 *ptr = data.data();
    *(reinterpret_cast<int *>(ptr)) = this->term;
    *(reinterpret_cast<int *>(ptr + 4)) = this->logic_index;
    std::vector<u8> content_data =
        this->content.serialize(this->content.size());
    memccpy((ptr + 8), content_data.data(), this->content.size());
    return;
  }

  void deserialize(std::vector<u8> &data) {
    if (data.size() != this->serialize_size()) {
      return;
    }
    u8 *ptr = data.data();
    this->term = *(reinterpret_cast<int *>(ptr));
    this->logic_index = *(reinterpret_cast<int *>(ptr + 4));
    std::vector<u8> content_data(data.begin() + 8, data.end());
    this->content.deserialize(content_data, this->content.size());
    return;
  }
};

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command> class RaftLog {
public:
  ~RaftLog();
  RaftLog(std::shared_ptr<BlockManager> bm);
  /* Lab3: Your code here */
  // MY_MODIFY:
  bool persist_metadata(int current_term, int leader_id);
  bool persist_log(std::vector<LogEntry<Command>> &log_list);
  bool persist_snapshot();
  bool recover(int &current_term, int &leader_id,
               std::vector<LogEntry<Command>> &log_list);
  bool need_recover();

private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  /* Lab3: Your code here */
  // MY_MODIFY: metadata在磁盘上的位置数据
  struct metadata_pos_config {
    int block_id;
    int offset;
    int length;
    void modify(int bid, int off, int len) {
      this->block_id = bid;
      this->offset = off;
      this->length = len;
    }
  };

  int log_num_per_block; // 记录每个块上容纳的log数量
  int log_ser_size;      // 记录一条log的对应序列化大小

  // MY_MODIFY:元数据在内存中的副本
  int valid; // 记录是否进行过持久化存储，若存储过则valid为1
  int current_term;
  int leader_id;
  // [start,end)
  int snapshot_start_block;
  int snapshot_end_block;
  int log_start_block;
  int log_num;

  // MY_MODIFY:元数据配置文件
  metadata_pos_config config_valid;
  metadata_pos_config config_current_term;
  metadata_pos_config config_leader_id;
  metadata_pos_config config_snapshot_start_block;
  metadata_pos_config config_snapshot_end_block;
  metadata_pos_config config_log_start_block;
  metadata_pos_config config_log_num;

  // MY_MODIFY:其他工具函数
  int get_int(u8 *data, int off);
  void flush_int(u8 *dst, int src, int off);
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) {
  /* Lab3: Your code here */
  this->bm_ = bm;
  // 更新配置文件
  int offset = 0;
  this->config_valid.modify(0, offset, sizeof(this->valid));
  offset += sizeof(this->valid);
  this->config_current_term.modify(0, offset, sizeof(this->current_term));
  offset += sizeof(this->current_term);
  this->config_leader_id.modify(0, offset, sizeof(this->leader_id));
  offset += sizeof(this->leader_id);
  this->config_snapshot_start_block.modify(0, offset,
                                           sizeof(this->snapshot_start_block));
  offset += sizeof(this->snapshot_start_block);
  this->config_snapshot_end_block.modify(0, offset,
                                         sizeof(this->snapshot_end_block));
  offset += sizeof(this->snapshot_end_block);
  this->config_log_start_block.modify(0, offset, sizeof(this->log_start_block));
  offset += sizeof(this->log_start_block);
  this->config_log_num.modify(0, offset, sizeof(this->log_num));

  u8 *metadata_block = new u8[this->bm_->block_size()];
  this->bm_->read_block(0, metadata_block);
  // 获取valid
  this->valid = this->get_int(metadata_block, config_valid.offset);
  if (this->valid == 1) {
    // 若valid=1,则从文件中恢复metadata
    this->current_term =
        this->get_int(metadata_block, config_current_term.offset);
    this->leader_id = this->get_int(metadata_block, config_leader_id.offset);
    this->snapshot_start_block =
        this->get_int(metadata_block, config_snapshot_start_block.offset);
    this->snapshot_end_block =
        this->get_int(metadata_block, config_snapshot_end_block.offset);
    this->log_start_block =
        this->get_int(metadata_block, config_log_start_block.offset);
    this->log_num = this->get_int(metadata_block, config_log_num.offset);
  } else {
    // 否则初始化，并刷入块中
    this->valid = 1;
    this->current_term = 0;
    this->leader_id = -1;
    this->snapshot_start_block = 1;
    this->snapshot_end_block = 2;
    this->log_start_block = 2;
    this->log_num = 0;
    this->flush_int(metadata_block, this->valid, this->config_valid.offset);
    this->flush_int(metadata_block, this->current_term,
                    this->config_current_term.offset);
    this->flush_int(metadata_block, this->leader_id,
                    this->config_leader_id.offset);
    this->flush_int(metadata_block, this->snapshot_start_block,
                    this->config_snapshot_start_block.offset);
    this->flush_int(metadata_block, this->snapshot_end_block,
                    this->config_snapshot_end_block.offset);
    this->flush_int(metadata_block, this->log_start_block,
                    this->config_log_start_block.offset);
    this->flush_int(metadata_block, this->log_num, this->config_log_num.offset);
    this->bm_->write_block(0, metadata_block);
    this->bm_->sync(0);
  }
  delete[] metadata_block;
  // 计算每块block中至多存储的log数量
  LogEntry<Command> tmp;
  this->log_ser_size = tmp.serialize_size();
  this->log_num_per_block = this->bm_->block_size() / this->log_ser_size;
}

template <typename Command> RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
  this->bm_.reset();
}

/* Lab3: Your code here */
// MY_MODIFY:
template <typename Command>
bool RaftLog<Command>::persist_metadata(int current_term, int leader_id) {
  int *data = new int[2];
  data[0] = current_term;
  data[1] = leader_id;

  // 上锁
  std::unique_lock<std::mutex> lock(this->mtx);
  this->bm_->write_partial_block(
      config_current_term.block_id, reinterpret_cast<u8 *>(data),
      config_current_term.offset, config_current_term.length);
  this->bm_->write_partial_block(
      config_leader_id.block_id, reinterpret_cast<u8 *>(data + 1),
      config_leader_id.offset, config_leader_id.length);
  this->bm_->sync(0);
  // NOTE: no fault process
  delete[] data;
  lock.unlock();
  return true;
}

template <typename Command>
bool RaftLog<Command>::persist_log(std::vector<LogEntry<Command>> &log_list) {
  // 上锁
  std::unique_lock<std::mutex> lock(this->mtx);
  int current_block_id = this->log_start_block;
  int current_off = 0;
  std::vector<u8> log_ser;
  // 持久化存储实际log数据
  bool is_first = true;
  for (LogEntry<Command> item : log_list) {
    // NOTE: 第一个log item是头节点，不会持久化存储！！！
    if (is_first) {
      is_first = false;
      continue;
    }
    log_ser.clear();
    item.serialize(log_ser);
    this->bm_->write_partial_block(current_block_id, log_ser.data(),
                                   current_off * (this->log_ser_size),
                                   this->log_ser_size);
    current_off += 1;
    if (current_off == this->log_num_per_block) {
      current_off = 0;
      current_block_id += 1;
    }
  }
  // log数据落盘
  for (int i = this->log_start_block; i <= current_block_id; ++i) {
    this->bm_->sync(i);
  }

  // 修改与log有关的metadata
  this->log_num = log_list.size() - 1;
  // log相关metadata持久化存储
  this->bm_->write_partial_block(0, reinterpret_cast<u8 *>(&(this->log_num)),
                                 config_log_num.offset, config_log_num.length);
  this->bm_->sync(0);
  lock.unlock();
  return true;
}

template <typename Command> bool RaftLog<Command>::persist_snapshot() {
  // 上锁
  std::unique_lock<std::mutex> lock(this->mtx);

  lock.unlock();
  return true;
}

template <typename Command>
bool RaftLog<Command>::recover(int &current_term, int &leader_id,
                               std::vector<LogEntry<Command>> &log_list) {
  // 上锁
  std::unique_lock<std::mutex> lock(this->mtx);
  current_term = this->current_term;
  leader_id = this->leader_id;
  // TODO: get log
  // 传入log_list必须为空
  if (!log_list.empty()) {
    lock.unlock();
    return false;
  }
  // 推入头节点
  LogEntry<Command> obj;
  log_list.push_back(obj);

  // 推入剩余节点
  int current_block_id = this->log_start_block;
  int current_off = 0;
  std::vector<u8> page_data(this->bm_->block_size());
  this->bm_->read_block(current_block_id, page_data.data());
  for (int i = 0; i < this->log_num; ++i) {
    std::vector<u8> log_ser(
        page_data.begin() + (current_off * this->log_ser_size),
        page_data.begin() + ((current_off + 1) * this->log_ser_size));
    obj.deserialize(log_ser);
    log_list.push_back(obj);

    ++current_off;
    if (current_off == this->log_num_per_block) {
      current_off = 0;
      ++current_block_id;
      this->bm_->read_block(current_block_id, page_data.data());
    }
  }

  lock.unlock();
  return true;
}

template <typename Command> bool RaftLog<Command>::need_recover() {
  std::unique_lock<std::mutex> lock(this->mtx);
  return this->valid == 1;
}

template <typename Command> int RaftLog<Command>::get_int(u8 *data, int off) {
  u8 *tmp = data + off;
  return *(reinterpret_cast<int *>(tmp));
}

template <typename Command>
void RaftLog<Command>::flush_int(u8 *dst, int src, int off) {
  int *obj = new int(src);
  u8 *pos = dst + off;
  memcpy(reinterpret_cast<char *>(pos), reinterpret_cast<char *>(obj),
         sizeof(int));
  delete obj;
}

} /* namespace chfs */

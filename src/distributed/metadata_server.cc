#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  this->mk_unlink_mutex.lock();
  ChfsResult<inode_id_t> mknode_result =
      this->operation_->mk_helper(parent, name.data(), (chfs::InodeType)type);
  this->mk_unlink_mutex.unlock();

  if (mknode_result.is_ok()) {
    return mknode_result.unwrap();
  } else {
    // NOTE: if return 0, then error
    return 0;
  }
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.

  this->mk_unlink_mutex.lock();
  // get unlink file inode id
  ChfsResult<inode_id_t> lookup_result =
      this->operation_->lookup(parent, name.data());
  if (lookup_result.is_err()) {
    this->mk_unlink_mutex.unlock();
    return false;
  }
  inode_id_t unlink_inode_id = lookup_result.unwrap();

  // get unlink file inode type
  ChfsResult<InodeType> gettype_res =
      this->operation_->gettype(unlink_inode_id);
  if (gettype_res.is_err()) {
    this->mk_unlink_mutex.unlock();
    return false;
  }
  InodeType unlink_inode_type = gettype_res.unwrap();

  if (unlink_inode_type == InodeType::Directory) {
    // can use unlink function in lab1
    ChfsNullResult unlink_res = this->operation_->unlink(parent, name.data());
    if (unlink_res.is_err()) {
      this->mk_unlink_mutex.unlock();
      return false;
    }

  } else {
    // free the remote block
    std::vector<BlockInfo> free_block_list;
    this->operation_->regular_get_blockinfo_list(unlink_inode_id,
                                                 free_block_list);
    for (auto item : free_block_list) {
      auto mid = std::get<1>(item);
      auto bid = std::get<0>(item);
      auto res = (this->clients_).at(mid)->call("free_block", bid);
      if (res.is_err()) {
        this->mk_unlink_mutex.unlock();
        return false;
      }
      bool res_value = res.unwrap()->as<bool>();
      if (!res_value) {
        this->mk_unlink_mutex.unlock();
        return false;
      }
    }
    // free inode and delete entry in parent
    ChfsNullResult unlink_res =
        this->operation_->regular_unlink_wo_block(parent, name);
    if (unlink_res.is_err()) {
      this->mk_unlink_mutex.unlock();
      return false;
    }
  }
  this->mk_unlink_mutex.unlock();
  return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  ChfsResult<inode_id_t> lookup_result =
      this->operation_->lookup(parent, name.data());

  if (lookup_result.is_ok()) {
    return lookup_result.unwrap();
  } else {
    // NOTE: if return 0, then error
    return 0;
  }
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  std::vector<BlockInfo> result;
  ChfsNullResult get_map_res =
      this->operation_->regular_get_blockinfo_list(id, result);
  if (get_map_res.is_err()) {
    // FIXME: how to handle error
    return result;
  } else {
    return result;
  }
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  mk_unlink_mutex.lock();
  mac_id_t machine_id = (this->generator).rand(1, this->num_data_servers);
  auto res = (this->clients_).at(machine_id)->call("alloc_block");
  if (res.is_err()) {
    // FIXME: how to handle error
    mk_unlink_mutex.unlock();
    return BlockInfo(0, 0, 0);
  }
  auto [block_id, version] =
      res.unwrap()->as<std::pair<block_id_t, version_t>>();
  // TODO:
  BlockInfo blockinfo(block_id, machine_id, version);
  // FIXME: how to handle error
  this->operation_->regular_add_blockinfo(id, blockinfo);
  mk_unlink_mutex.unlock();
  return blockinfo;
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  mk_unlink_mutex.lock();
  auto res = (this->clients_).at(machine_id)->call("free_block", block_id);
  if (res.is_err()) {
    mk_unlink_mutex.unlock();
    return false;
  }
  bool res_value = res.unwrap()->as<bool>();
  if (!res_value) {
    mk_unlink_mutex.unlock();
    return false;
  }
  ChfsNullResult inode_modify_res =
      this->operation_->regular_remove_blockinfo(id, block_id, machine_id);
  if (inode_modify_res.is_err()) {
    mk_unlink_mutex.unlock();
    return false;
  }
  mk_unlink_mutex.unlock();
  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  std::list<chfs::DirectoryEntry> list;
  ChfsNullResult read_dir_result =
      read_directory((this->operation_).get(), node, list);
  if (read_dir_result.is_ok()) {
    std::vector<std::pair<std::string, inode_id_t>> return_value;
    for (auto it = list.begin(); it != list.end(); ++it) {
      auto item = std::pair<std::string, inode_id_t>(it->name, it->id);
      return_value.push_back(item);
    }
    return return_value;
  } else {
    // FIXME: how to handle error
    std::vector<std::pair<std::string, inode_id_t>> return_value;
    return return_value;
  }
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  // Tuple of <size, atime, mtime, ctime, type>
  ChfsResult<std::pair<InodeType, FileAttr>> get_type_attr_result =
      this->operation_->get_type_attr(id);
  if (get_type_attr_result.is_ok()) {
    std::pair<InodeType, FileAttr> pair_object = get_type_attr_result.unwrap();
    return std::tuple<u64, u64, u64, u64, u8>(
        pair_object.second.size, pair_object.second.atime,
        pair_object.second.mtime, pair_object.second.ctime,
        (u8)(pair_object.first));
  } else {
    // FIXME: how to handle error
    return std::tuple<u64, u64, u64, u64, u8>(0, 0, 0, 0, 0);
  }
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs
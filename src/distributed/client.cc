#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto mknode_res =
      this->metadata_server_->call("mknode", (u8)type, parent, name);
  if (mknode_res.is_err()) {
    return ChfsResult<inode_id_t>(mknode_res.unwrap_error());
  }
  inode_id_t inode_id = mknode_res.unwrap()->as<inode_id_t>();
  if (inode_id == 0) {
    return ChfsResult<inode_id_t>(ErrorType::OUT_OF_RESOURCE);
  }
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  auto unlink_res = this->metadata_server_->call("unlink", parent, name);
  if (unlink_res.is_err()) {
    return ChfsNullResult(unlink_res.unwrap_error());
  }
  bool is_success = unlink_res.unwrap()->as<bool>();
  if (is_success)
    return KNullOk;
  else
    return ChfsNullResult(ErrorType::DONE);
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto lookup_res = this->metadata_server_->call("lookup", parent, name);
  if (lookup_res.is_err()) {
    std::cout << "Error1" << std::endl;
    return ChfsResult<inode_id_t>(lookup_res.unwrap_error());
  }
  inode_id_t inode_id = lookup_res.unwrap()->as<inode_id_t>();
  if (inode_id == 0) {
    std::cout << "Error2" << std::endl;
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  } else {
    return ChfsResult<inode_id_t>(inode_id);
  }
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  auto readdir_res = this->metadata_server_->call("readdir", id);
  if (readdir_res.is_err()) {
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(
        readdir_res.unwrap_error());
  }
  std::vector<std::pair<std::string, inode_id_t>> dir_content =
      readdir_res.unwrap()
          ->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(
      dir_content);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  auto getta_res = this->metadata_server_->call("get_type_attr", id);
  if (getta_res.is_err()) {
    return ChfsResult<std::pair<InodeType, FileAttr>>(getta_res.unwrap_error());
  }
  std::tuple<u64, u64, u64, u64, u8> type_attr =
      getta_res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  InodeType inode_type;
  FileAttr file_attr;
  if (std::get<4>(type_attr) == (u8)FileType::REGULAR)
    inode_type = InodeType::FILE;
  else
    inode_type = InodeType::Directory;
  file_attr.atime = std::get<1>(type_attr);
  file_attr.ctime = std::get<2>(type_attr);
  file_attr.mtime = std::get<3>(type_attr);
  file_attr.size = std::get<0>(type_attr);
  return ChfsResult<std::pair<InodeType, FileAttr>>(
      std::pair<InodeType, FileAttr>(inode_type, file_attr));
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  if (size == 0) {
    return ChfsResult<std::vector<u8>>(std::vector<u8>());
  }

  std::vector<u8> result;
  auto block_map_res = this->metadata_server_->call("get_block_map", id);
  if (block_map_res.is_err()) {
    return ChfsResult<std::vector<u8>>(block_map_res.unwrap_error());
  }
  std::vector<BlockInfo> block_map =
      block_map_res.unwrap()->as<std::vector<BlockInfo>>();
  u32 block_num = block_map.size();
  if (offset + size - 1 >= block_num * DiskBlockSize) {
    // read data is over
    return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);
  }

  u32 start_block_num = offset / DiskBlockSize;
  u32 end_block_num = (offset + size - 1) / DiskBlockSize;

  for (int i = start_block_num; i <= end_block_num; ++i) {
    auto bid = std::get<0>(block_map[i]);
    auto mid = std::get<1>(block_map[i]);
    auto ver = std::get<2>(block_map[i]);

    u32 inblock_start = 0;
    u32 inblock_end = DiskBlockSize - 1;

    if (i == start_block_num)
      inblock_start = offset % DiskBlockSize;
    if (i == end_block_num)
      inblock_end = (offset + size - 1) % DiskBlockSize;

    auto read_res = (this->data_servers_[mid])
                        ->call("read_data", bid, inblock_start,
                               inblock_end - inblock_start + 1, ver);
    if (read_res.is_err()) {
      return ChfsResult<std::vector<u8>>(read_res.unwrap_error());
    }
    std::vector<u8> read_content = read_res.unwrap()->as<std::vector<u8>>();
    if (read_content.size() == 0) {
      // version is old
      return ChfsResult<std::vector<u8>>(ErrorType::INVALID);
    }
    // insert the content to the end of result
    result.insert(result.end(), read_content.begin(), read_content.end());
  }

  return ChfsResult<std::vector<u8>>(result);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  if (data.empty()) {
    return KNullOk;
  }

  // check weather need to alloc
  auto type_attr_res = this->get_type_attr(id);
  if (type_attr_res.is_err()) {
    return ChfsNullResult(type_attr_res.unwrap_error());
  }
  auto old_size = type_attr_res.unwrap().second.size;

  auto size = data.size();

  while (offset + size - 1 >= old_size) {
    auto alloc_res = this->metadata_server_->call("alloc_block", id);
    if (alloc_res.is_err()) {
      return ChfsNullResult(alloc_res.unwrap_error());
    }
    old_size += DiskBlockSize;
  }

  // get block map
  auto block_map_res = this->metadata_server_->call("get_block_map", id);
  if (block_map_res.is_err()) {
    return ChfsNullResult(block_map_res.unwrap_error());
  }
  std::vector<BlockInfo> block_map =
      block_map_res.unwrap()->as<std::vector<BlockInfo>>();

  // write with enough block
  u32 start_block_num = offset / DiskBlockSize;
  u32 end_block_num = (offset + size - 1) / DiskBlockSize;
  u64 have_write_byte = 0;
  for (auto i = start_block_num; i <= end_block_num; ++i) {
    auto bid = std::get<0>(block_map[i]); /////
    auto mid = std::get<1>(block_map[i]);

    u32 inblock_start = 0;
    u32 inblock_end = DiskBlockSize - 1;
    if (i == start_block_num)
      inblock_start = offset % DiskBlockSize;
    if (i == end_block_num)
      inblock_end = (offset + size - 1) % DiskBlockSize;

    u32 write_size = inblock_end - inblock_start + 1;
    std::vector<u8> buffer;
    buffer.insert(buffer.end(), data.begin() + have_write_byte,
                  data.begin() + have_write_byte + write_size);

    auto write_res = (this->data_servers_[mid])
                         ->call("write_data", bid, inblock_start, buffer);
    if (write_res.is_err()) {
      return ChfsNullResult(write_res.unwrap_error());
    }
    bool is_succeed = write_res.unwrap()->as<bool>();
    if (!is_succeed) {
      return ChfsNullResult(ErrorType::DONE);
    }
    have_write_byte += write_size;
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  // NOTE: this call will also free corresponding block in dataserver
  auto free_block_res =
      this->metadata_server_->call("free_block", id, block_id, mac_id);
  if (free_block_res.is_err()) {
    return ChfsNullResult(free_block_res.unwrap_error());
  }
  bool is_succeed = free_block_res.unwrap()->as<bool>();
  if (is_succeed)
    return KNullOk;
  else
    return ChfsNullResult(ErrorType::DONE);
}

} // namespace chfs
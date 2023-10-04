#include <algorithm>
#include <regex>
#include <sstream>
#include <vector>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  std::string entry =
      (src.empty() ? "" : "/") + filename + ":" + std::to_string(id);
  src += entry;

  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  if (src.empty()) {
    return;
  }

  std::regex re("[:/]");
  std::sregex_token_iterator it(src.begin(), src.end(), re, -1);
  std::sregex_token_iterator end;
  while (it != end) {
    DirectoryEntry tmp;
    tmp.name = *it;
    it++;
    tmp.id = std::stoull(*it);
    it++;
    list.push_back(tmp);
  }
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  std::list<DirectoryEntry> list(0);
  parse_directory(src, list);
  for (std::list<DirectoryEntry>::iterator it = list.begin(); it != list.end();
       ++it) {
    if (it->name == filename) {
      list.erase(it);
      break;
    }
  }
  res = dir_list_to_string(list);
  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {

  // TODO: Implement this function.
  ChfsResult<std::vector<u8>> res = fs->read_file(id);
  if (res.is_err()) {
    return ChfsNullResult(ErrorType::NotExist);
  }
  std::string content(reinterpret_cast<const char *>(res.unwrap().data()),
                      res.unwrap().size());
  parse_directory(content, list);
  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  std::string filename(name, strlen(name));
  read_directory(this, id, list);
  for (std::list<DirectoryEntry>::iterator it = list.begin(); it != list.end();
       ++it) {
    if (it->name == filename) {
      return ChfsResult<inode_id_t>(it->id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  std::list<DirectoryEntry> list;
  std::string filename(name, strlen(name));
  read_directory(this, id, list);
  for (std::list<DirectoryEntry>::iterator it = list.begin(); it != list.end();
       ++it) {
    if (it->name == filename) {
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }
  }

  ChfsResult<block_id_t> block_id = block_allocator_->allocate();
  if (block_id.is_err()) {
    return ChfsResult<inode_id_t>(ErrorType::OUT_OF_RESOURCE);
  }
  ChfsResult<inode_id_t> inode_id =
      inode_manager_->allocate_inode(type, block_id.unwrap());
  if (inode_id.is_err()) {
    return ChfsResult<inode_id_t>(ErrorType::OUT_OF_RESOURCE);
  }

  DirectoryEntry direntry;
  direntry.name = filename;
  direntry.id = inode_id.unwrap();
  list.push_back(direntry);
  std::string list_stirng = dir_list_to_string(list);
  std::vector<u8> buffer(list_stirng.begin(), list_stirng.end());
  write_file(id, buffer);

  return ChfsResult<inode_id_t>(inode_id.unwrap());
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO:
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  std::list<DirectoryEntry> list;
  std::string filename(name, strlen(name));
  read_directory(this, parent, list);
  for (std::list<DirectoryEntry>::iterator it = list.begin(); it != list.end();
       ++it) {
    if (it->name == filename) {
      ChfsResult<InodeType> inode_type = gettype(it->id);
      if (inode_type.unwrap() == InodeType::Directory) {
        ChfsResult<FileAttr> attr = getattr(it->id);
        u64 filesize = attr.unwrap().size;

        if (filesize != 0) {
          return ChfsNullResult(ErrorType::NotEmpty);
        }

        ChfsNullResult res = remove_file(it->id);
        if (res.is_ok()) {
          return KNullOk;
        }
      } else {
        ChfsNullResult res = remove_file(it->id);
        if (res.is_ok()) {
          return KNullOk;
        }
      }
    }
  }
  return ChfsNullResult(ErrorType::NotExist);
  ;
}

} // namespace chfs

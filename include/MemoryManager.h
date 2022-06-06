#pragma once
#include "ByteArray.h"
#include "json.hpp"
#include <cassert>
#include <iostream>
#include <map>
#include <optional>
#include <vector>

namespace kvaaas {

enum MemoryPurpose {
  BEGIN = 0,
  KVS = 0,
  SST = 1,
  SKIP_LIST_UL = 2,
  SKIP_LIST_BL = 3,
  SKIP_LIST_UL_H = 4,
  END = 5,
};

inline std::string to_string(MemoryPurpose p) {
  switch (p) {
  case MemoryPurpose::KVS:
    return "_kvs";
  case MemoryPurpose::SST:
    return "_sst";
  case MemoryPurpose::SKIP_LIST_UL:
    return "_skip_list_ul";
  case MemoryPurpose::SKIP_LIST_BL:
    return "_skip_list_bl";
  case MemoryPurpose::SKIP_LIST_UL_H:
    return "_skip_list_ul_h";
  default:
    std::cerr << "Unreachable! Incorrect MemoryPurpose!";
  }
  return "";
}

class MemoryType {
private:
  MemoryPurpose mp;
  std::optional<std::size_t> sst_level;

public:
  explicit MemoryType(MemoryPurpose mp_,
                      std::optional<std::size_t> sst_level_ = {});

  [[nodiscard]] MemoryPurpose get_memory_purpose() const;

  [[nodiscard]] std::size_t get_sst_level() const;

  [[nodiscard]] bool has_sst_level() const;
};

bool cmp_memory_type(MemoryType memory_type1, MemoryType memory_type2);
using cmp_memory_type_type = bool (*)(MemoryType, MemoryType);

class MemoryManager {
public:
  virtual ByteArrayPtr get_byte_array(MemoryPurpose memory_purpose,
                                      std::optional<std::size_t> sst_level) = 0;

  virtual ByteArrayPtr
  create_byte_array(MemoryPurpose memory_purpose,
                    std::optional<std::size_t> sst_level) = 0;

  virtual ByteArrayPtr
  start_overwrite(MemoryPurpose memory_purpose,
                  std::optional<std::size_t> sst_level) = 0;

  virtual void end_overwrite(MemoryPurpose memory_purpose,
                             std::optional<std::size_t> sst_level) = 0;

  virtual void remove(MemoryPurpose memory_purpose,
                      std::optional<std::size_t> sst_level) = 0;

  virtual ByteArrayPtr get_byte_array(MemoryPurpose memory_purpose) = 0;

  virtual ByteArrayPtr create_byte_array(MemoryPurpose memory_purpose) = 0;

  virtual ByteArrayPtr start_overwrite(MemoryPurpose memory_purpose) = 0;

  virtual void end_overwrite(MemoryPurpose memory_purpose) = 0;

  virtual void remove(MemoryPurpose memory_purpose) = 0;

  virtual ~MemoryManager() = default;
};

class RAMMemoryManager : public MemoryManager {
private:
  std::map<MemoryType, ByteArrayPtr, cmp_memory_type_type> memory{
      cmp_memory_type};
  std::map<MemoryType, ByteArrayPtr, cmp_memory_type_type> memory_to_overwrite{
      cmp_memory_type};

public:
  RAMMemoryManager() = default;
  RAMMemoryManager(const RAMMemoryManager &) = delete;
  RAMMemoryManager &operator=(const RAMMemoryManager &) = delete;
  RAMMemoryManager(RAMMemoryManager &&) = default;
  RAMMemoryManager &operator=(RAMMemoryManager &&) = default;

  ByteArrayPtr get_byte_array(MemoryPurpose memory_purpose,
                              std::optional<std::size_t> sst_level) override;

  ByteArrayPtr create_byte_array(MemoryPurpose memory_purpose,
                                 std::optional<std::size_t> sst_level) override;
  ByteArrayPtr start_overwrite(MemoryPurpose memory_purpose,
                               std::optional<std::size_t> sst_level) override;

  void end_overwrite(MemoryPurpose memory_purpose,
                     std::optional<std::size_t> sst_level) override;

  void remove(MemoryPurpose memory_purpose,
              std::optional<std::size_t> sst_level) override;

  ByteArrayPtr get_byte_array(MemoryPurpose memory_purpose) override;

  ByteArrayPtr create_byte_array(MemoryPurpose memory_purpose) override;

  ByteArrayPtr start_overwrite(MemoryPurpose memory_purpose) override;

  void end_overwrite(MemoryPurpose memory_purpose) override;

  void remove(MemoryPurpose memory_purpose) override;

  ~RAMMemoryManager() noexcept override;
};

struct Upload {};

class FileMemoryManager : public MemoryManager {
private:
  std::map<MemoryType, FileByteArrayPtr, cmp_memory_type_type> memory{
      cmp_memory_type};
  std::map<MemoryType, FileByteArrayPtr, cmp_memory_type_type>
      memory_to_overwrite{cmp_memory_type};

public:
  FileMemoryManager(std::string root);
  FileMemoryManager(Upload, std::string root);
  FileMemoryManager(nlohmann::json mem_json, std::string root);
  FileMemoryManager(const FileMemoryManager &) = delete;
  FileMemoryManager &operator=(const FileMemoryManager &) = delete;
  FileMemoryManager(FileMemoryManager &&) = default;
  FileMemoryManager &operator=(FileMemoryManager &&) = default;

  ByteArrayPtr get_byte_array(MemoryPurpose memory_purpose,
                              std::optional<std::size_t> sst_level) override;

  ByteArrayPtr create_byte_array(MemoryPurpose memory_purpose,
                                 std::optional<std::size_t> sst_level) override;

  ByteArrayPtr start_overwrite(MemoryPurpose memory_purpose,
                               std::optional<std::size_t> sst_level) override;

  void end_overwrite(MemoryPurpose memory_purpose,
                     std::optional<std::size_t> sst_level) override;

  void remove(MemoryPurpose memory_purpose,
              std::optional<std::size_t> sst_level) override;

  ByteArrayPtr get_byte_array(MemoryPurpose memory_purpose) override;

  ByteArrayPtr create_byte_array(MemoryPurpose memory_purpose) override;

  ByteArrayPtr start_overwrite(MemoryPurpose memory_purpose) override;

  void end_overwrite(MemoryPurpose memory_purpose) override;

  void remove(MemoryPurpose memory_purpose) override;

  ~FileMemoryManager() noexcept override;

  static FileMemoryManager from_dir(std::string);

private:
  std::string generate_new_filename(MemoryPurpose);
  inline void update_manifest() {
    std::ofstream os(root + "/manifest.json");
    os << manifest_json;
  };
  std::string root;
  nlohmann::json manifest_json; // assume it cannot down during overwriting
};
} // namespace kvaaas

#include "MemoryManager.h"
#include "ByteArray.h"
#include <cassert>
#include <optional>
#include <vector>

#include "json.hpp"

namespace kvaaas {

MemoryType::MemoryType(MemoryPurpose mp_, std::optional<std::size_t> sst_level_)
    : mp(mp_), sst_level(sst_level_) {
  if (mp_ == MemoryPurpose::SST) {
    assert(sst_level.has_value());
  }
}

MemoryPurpose MemoryType::get_memory_purpose() const { return mp; }

std::size_t MemoryType::get_sst_level() const { return sst_level.value(); }

bool MemoryType::has_sst_level() const { return sst_level.has_value(); }

bool cmp_memory_type(MemoryType memory_type1, MemoryType memory_type2) {
  return memory_type1.get_memory_purpose() <
             memory_type2.get_memory_purpose() ||
         (memory_type1.get_memory_purpose() ==
              memory_type2.get_memory_purpose() &&
          memory_type1.has_sst_level() && memory_type2.has_sst_level() &&
          memory_type1.get_sst_level() < memory_type2.get_sst_level());
}

ByteArrayPtr RAMMemoryManager::get_file(MemoryPurpose memory_purpose,
                                        std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  return memory.at(memory_type);
}

ByteArrayPtr
RAMMemoryManager::create_file(MemoryPurpose memory_purpose,
                              std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  assert(memory.count(memory_type) == 0);
  memory[memory_type] = ::new RAMByteArray();
  return memory[memory_type];
}

ByteArrayPtr
RAMMemoryManager::start_overwrite(MemoryPurpose memory_purpose,
                                  std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  assert(memory_to_overwrite.count(memory_type) == 0);
  memory_to_overwrite[memory_type] = ::new RAMByteArray();
  return memory_to_overwrite[memory_type];
}

void RAMMemoryManager::end_overwrite(MemoryPurpose memory_purpose,
                                     std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  delete memory[memory_type];
  memory[memory_type] = memory_to_overwrite[memory_type];
  memory_to_overwrite.erase(memory_type);
}

void RAMMemoryManager::remove(MemoryPurpose memory_purpose,
                              std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  delete memory[memory_type];
  memory.erase(memory_type);
}

ByteArrayPtr RAMMemoryManager::get_file(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  return memory.at(memory_type);
}

ByteArrayPtr RAMMemoryManager::create_file(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  assert(memory.count(memory_type) == 0);
  memory[memory_type] = ::new RAMByteArray();
  return memory[memory_type];
}

ByteArrayPtr RAMMemoryManager::start_overwrite(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  assert(memory_to_overwrite.count(memory_type) == 0);
  memory_to_overwrite[memory_type] = ::new RAMByteArray();
  return memory_to_overwrite[memory_type];
}

void RAMMemoryManager::end_overwrite(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  delete memory[memory_type];
  memory[memory_type] = memory_to_overwrite[memory_type];
  memory_to_overwrite.erase(memory_type);
}

void RAMMemoryManager::remove(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  delete memory[memory_type];
  memory.erase(memory_type);
}

RAMMemoryManager::~RAMMemoryManager() noexcept {
  for (auto [_, ptr] : memory) {
    delete ptr;
  }
  for (auto [_, ptr] : memory_to_overwrite) {
    delete ptr;
  }
}

// FileMemoryManager

FileMemoryManager::FileMemoryManager(std::string root) : root(root) {

}


FileMemoryManager::FileMemoryManager(nlohmann::json mem_json, nlohmann::json mem_to_ov, std::string root) {
  
}


std::string FileMemoryManager::generate_new_filename(MemoryPurpose mp) {
  return root + "/file" + to_string(mp) + std::to_string(_cnt++);
}

ByteArrayPtr FileMemoryManager::get_file(MemoryPurpose memory_purpose,
                                         std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  return memory.at(memory_type);
}

ByteArrayPtr
FileMemoryManager::create_file(MemoryPurpose memory_purpose,
                               std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  assert(memory.count(memory_type) == 0);
  memory[memory_type] =
      ::new FileByteArray(generate_new_filename(memory_purpose));

  return memory[memory_type];
}

ByteArrayPtr
FileMemoryManager::start_overwrite(MemoryPurpose memory_purpose,
                                   std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  assert(memory_to_overwrite.count(memory_type) == 0);
  memory_to_overwrite[memory_type] =
      ::new FileByteArray(generate_new_filename(memory_purpose));
  return memory_to_overwrite[memory_type];
}

void FileMemoryManager::end_overwrite(MemoryPurpose memory_purpose,
                                      std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  delete memory[memory_type];
  memory[memory_type] = memory_to_overwrite[memory_type];
  memory_to_overwrite.erase(memory_type);
}

void FileMemoryManager::remove(MemoryPurpose memory_purpose,
                               std::optional<std::size_t> sst_level) {
  MemoryType memory_type(memory_purpose, sst_level);
  delete memory[memory_type];
  memory.erase(memory_type);
}

ByteArrayPtr FileMemoryManager::get_file(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  return memory.at(memory_type);
}

ByteArrayPtr FileMemoryManager::create_file(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  assert(memory.count(memory_type) == 0);
  memory[memory_type] =
      ::new FileByteArray(generate_new_filename(memory_purpose));
  return memory[memory_type];
}

ByteArrayPtr FileMemoryManager::start_overwrite(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  assert(memory_to_overwrite.count(memory_type) == 0);
  memory_to_overwrite[memory_type] =
      ::new FileByteArray(generate_new_filename(memory_purpose));
  return memory_to_overwrite[memory_type];
}

void FileMemoryManager::end_overwrite(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  delete memory[memory_type];
  memory[memory_type] = memory_to_overwrite[memory_type];
  memory_to_overwrite.erase(memory_type);
}

void FileMemoryManager::remove(MemoryPurpose memory_purpose) {
  MemoryType memory_type(memory_purpose);
  delete memory[memory_type];
  memory.erase(memory_type);
}

FileMemoryManager::~FileMemoryManager() noexcept {
  for (auto [_, ptr] : memory) {
    delete ptr;
  }
  for (auto [_, ptr] : memory_to_overwrite) {
    delete ptr;
  }
}


static FileMemoryManager from_file(std::string root) {
  using namespace nlohmann;
  std::string mem_name = root + "/" + "mememory.json";
  std::string mem_to_overwrite_name = root + "/" + "mememory_to_overwrite.json";
 
  std::ifstream imem(mem_name);
  nlohmann::json memory_json;
  imem >> memory_json;

  imem = std::ifstream(mem_to_overwrite_name);
  nlohmann::json memory_to_ov_json;
  imem >> memory_to_ov_json;


  return FileMemoryManager(memory_json, memory_to_ov_json,root);
}

} // namespace kvaaas

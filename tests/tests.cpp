#include "ByteArray.h"
#include "MemoryManager.h"
#include "BloomFilter.h"
#include "doctest.h"
#include <memory>

using namespace kvaaas;

namespace {
std::vector<ByteType> bytes(std::initializer_list<unsigned char> bytes) {
  std::vector<ByteType> res(bytes.size());
  auto it = bytes.begin();
  for (std::size_t i = 0; i < bytes.size(); ++i, ++it) {
    res[i] = static_cast<ByteType>(*it);
  }
  return res;
}
} // namespace

TEST_CASE("Simple test") {
  MemoryType mt(MemoryPurpose::SST, 4);
  CHECK(mt.get_sst_level() == 4);
  CHECK(mt.get_memory_purpose() == MemoryPurpose::SST);
}

TEST_CASE("RAMByteArray") {
  RAMByteArray ram_arr;
  ByteArray &arr = ram_arr;
  CHECK(arr.size() == 0);
  arr.append(bytes({0, 1, 2, 3}));
  CHECK(arr.size() == 4);
  CHECK(arr.read(0, 1) == bytes({0}));
  CHECK(arr.read(0, 4) == bytes({0, 1, 2, 3}));
  CHECK(arr.read(3, 4) == bytes({3}));
  CHECK(arr.read(3, 4) == bytes({3}));
  arr.append(bytes({3, 4, 5}));
  CHECK(arr.size() == 7);
  CHECK(arr.read(3, 7) == bytes({3, 3, 4, 5}));
}

TEST_CASE("RAMMemoryManager non-copyable") {
  CHECK(!std::is_copy_constructible_v<RAMMemoryManager>);
  CHECK(!std::is_copy_assignable_v<RAMMemoryManager>);
}

TEST_CASE("RAMMemoryManager movable") {
  CHECK(std::is_move_constructible_v<RAMMemoryManager>);
  CHECK(std::is_move_assignable_v<RAMMemoryManager>);
}

TEST_CASE("RAMMemoryManager simple") {
  RAMMemoryManager ram_memory_manager;
  MemoryManager &memory_manager = ram_memory_manager;
  ByteArrayPtr kvs = memory_manager.create_file(MemoryPurpose::KVS);
  ByteArrayPtr skip_list_bl =
      memory_manager.create_file(MemoryPurpose::SKIP_LIST_BL);
  ByteArrayPtr skip_list_ul =
      memory_manager.create_file(MemoryPurpose::SKIP_LIST_UL);
  std::vector<ByteArrayPtr> sst;
  for (std::size_t i = 0; i < 20; ++i) {
    sst.push_back(memory_manager.create_file(MemoryPurpose::SST, i));
  }

  CHECK(kvs == memory_manager.get_file(MemoryPurpose::KVS));
  CHECK(skip_list_bl == memory_manager.get_file(MemoryPurpose::SKIP_LIST_BL));
  CHECK(skip_list_ul == memory_manager.get_file(MemoryPurpose::SKIP_LIST_UL));
  for (std::size_t i = 0; i < sst.size(); ++i) {
    CHECK(sst[i] == memory_manager.get_file(MemoryPurpose::SST, i));
  }

  sst[2]->append(bytes({3, 4, 5}));
  CHECK(memory_manager.get_file(MemoryPurpose::SST, 2)->read(0, 3) ==
        bytes({3, 4, 5}));

  kvs->append(bytes({1, 2, 3}));
  ByteArrayPtr new_kvs = memory_manager.start_overwrite(MemoryPurpose::KVS);
  new_kvs->append(bytes({3, 4, 6}));
  memory_manager.end_overwrite(MemoryPurpose::KVS);
  CHECK(new_kvs == memory_manager.get_file(MemoryPurpose::KVS));
  CHECK(new_kvs->read(1, 2) == bytes({4}));

  new_kvs = memory_manager.start_overwrite(MemoryPurpose::KVS);
  new_kvs->append(bytes({6, 7, 8}));
  memory_manager.end_overwrite(MemoryPurpose::KVS);
  CHECK(new_kvs == memory_manager.get_file(MemoryPurpose::KVS));
  CHECK(new_kvs->read(0, 3) == bytes({6, 7, 8}));
}

TEST_CASE("RAMMemoryManager move-constructor") {
  RAMMemoryManager memory_manager1;
  ByteArrayPtr kvs = memory_manager1.create_file(MemoryPurpose::KVS);
  ByteArrayPtr skip_list_bl =
      memory_manager1.create_file(MemoryPurpose::SKIP_LIST_BL);
  ByteArrayPtr skip_list_ul =
      memory_manager1.create_file(MemoryPurpose::SKIP_LIST_UL);
  std::vector<ByteArrayPtr> sst;
  for (std::size_t i = 0; i < 20; ++i) {
    sst.push_back(memory_manager1.create_file(MemoryPurpose::SST, i));
  }

  RAMMemoryManager memory_manager(std::move(memory_manager1));

  /*test actions*/ {
    CHECK(kvs == memory_manager.get_file(MemoryPurpose::KVS));
    CHECK(skip_list_bl == memory_manager.get_file(MemoryPurpose::SKIP_LIST_BL));
    CHECK(skip_list_ul == memory_manager.get_file(MemoryPurpose::SKIP_LIST_UL));
    for (std::size_t i = 0; i < sst.size(); ++i) {
      CHECK(sst[i] == memory_manager.get_file(MemoryPurpose::SST, i));
    }

    sst[2]->append(bytes({3, 4, 5}));
    CHECK(memory_manager.get_file(MemoryPurpose::SST, 2)->read(0, 3) ==
          bytes({3, 4, 5}));

    ByteArrayPtr new_sst_2 =
        memory_manager.start_overwrite(MemoryPurpose::SST, 2);

    new_sst_2->append(bytes({3, 4, 6}));
    memory_manager.end_overwrite(MemoryPurpose::SST, 2);
    CHECK(new_sst_2 == memory_manager.get_file(MemoryPurpose::SST, 2));
    CHECK(new_sst_2->read(1, 2) == bytes({4}));

    new_sst_2 = memory_manager.start_overwrite(MemoryPurpose::SST, 2);
    new_sst_2->append(bytes({6, 7, 8}));
    memory_manager.end_overwrite(MemoryPurpose::SST, 2);
    CHECK(new_sst_2 == memory_manager.get_file(MemoryPurpose::SST, 2));
    CHECK(new_sst_2->read(0, 3) == bytes({6, 7, 8}));
  }
}

TEST_CASE("BloomFilter") {
  BloomFilter filter(100); 
  KeyType key1 = KeyType{std::byte(1), std::byte(2), std::byte(3)};
  KeyType key2 = KeyType{std::byte(4), std::byte(5), std::byte(6)};
  filter.add(key1);
  filter.add(key2);
  CHECK(filter.has_key(key1));
  CHECK(filter.has_key(key2));
}

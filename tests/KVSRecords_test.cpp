#include "doctest.h"
#include <KVSRecordsViewer.h>
#include <iostream>
#include <random>

using namespace kvaaas;

namespace {
KVSRecord gen_random() {
  KVSRecord record{};

  std::random_device rd;
  std::mt19937_64 rnd(rd());
  std::uniform_int_distribution<> dist(10000, 50000);

  for (auto &v : record.key) {
    v = static_cast<ByteType>(rnd() & 0xFF);
  }
  record.is_deleted = static_cast<ByteType>((rnd() & 0xFF) > 0xA0 ? 1 : 0);
  record.value_size = dist(rnd);
  record.value = std::vector<ByteType>(record.value_size);
  for (auto &v : record.value) {
    v = static_cast<ByteType>(rnd() & 0xFF);
  }
  return record;
}
} // namespace

TEST_CASE("CREATE") {
  FileByteArray arr("fileArray", true);
  KVSRecordsViewer viewer(&arr, nullptr);
}

TEST_CASE("Single append") {
  FileByteArray arr("fileArray", true);
  KVSRecordsViewer viewer(&arr, nullptr);

  KVSRecord record = gen_random();
  viewer.append(record);
  KVSRecord read_record = viewer.read_record(0);

  CHECK(record.key == read_record.key);
  CHECK(record.is_deleted == read_record.is_deleted);
  CHECK(record.value_size == read_record.value_size);
  CHECK(record.value == record.value);
  CHECK(record.value == read_record.value);

  CHECK(record == read_record);
}

TEST_CASE("Multiple append") {
  FileByteArray arr("fileArray", true);
  KVSRecordsViewer viewer(&arr, nullptr);

  std::vector<KVSRecord> records(1000);
  for (auto &record : records) {
    record = gen_random();
    viewer.append(record);
  }

  std::uint64_t offset = 0;
  for (const auto &record : records) {
    auto record2 = viewer.read_record(offset);
    CHECK(record == record2);
    offset += KVSRecordsViewer::get_value_size(record2);
  }
}

TEST_CASE("Multiple is_deleted") {
  FileByteArray arr("fileArray", true);
  KVSRecordsViewer viewer(&arr, nullptr);

  std::vector<KVSRecord> records(1000);
  for (auto &record : records) {
    record = gen_random();
    viewer.append(record);
  }

  std::uint64_t offset = 0;
  for (const auto &record : records) {
    if (record.is_deleted == std::byte{1}) {
      CHECK(viewer.is_deleted(offset));
    }
    auto record2 = viewer.read_record(offset);
    offset += KVSRecordsViewer::get_value_size(record2);
  }
}
/*
TEST_CASE("Multiple markDeleted") {
  FileByteArray arr("fileArray", true);
  KVSRecordsViewer viewer(&arr, nullptr);

  std::vector<KVSRecord> records(1000);
  std::vector<uint64_t> marked_records(1000, 0);
  std::vector<uint64_t> marked_offsets;

  for (auto &record : records) {
    record = gen_random();
    record.is_deleted = ByteType{0};
    viewer.append(record);
  }

  std::random_device rd;
  std::mt19937_64 rnd(rd());
  std::uniform_int_distribution<> dist(0, 1);

  std::uint64_t offset = 0;
  for (uint64_t i = 0; i < records.size(); i++) {
    if (dist(rnd)) {
      viewer.mark_as_deleted(offset);
      marked_offsets.push_back(offset);
      marked_records[i] = 1;
    }
    auto record2 = viewer.read_record(offset);
    offset += KVSRecordsViewer::get_value_size(record2);
  }

  for (const auto &marked_offset : marked_offsets) {
    CHECK(viewer.is_deleted(marked_offset));
  }

  SUBCASE("Checking whether anything other is changed") {
    offset = 0;
    for (uint64_t i = 0; i < records.size(); i++) {
      KVSRecord read_record = viewer.read_record(offset);
      if (marked_records[i]) {
        CHECK(viewer.is_deleted(offset));
      } else {
        CHECK(!viewer.is_deleted(offset));
      }
      CHECK(records[i].key == read_record.key);
      CHECK(records[i].value_size == read_record.value_size);
      CHECK(records[i].value == read_record.value);
      offset += KVSRecordsViewer::get_value_size(records[i]);
    }
  }
}
*/
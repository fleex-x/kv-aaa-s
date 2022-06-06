#include "ByteArray.h"
#include "SkipList.h"
#include "SkipListRecords.h"
#include "doctest.h"
#include <algorithm>
#include <climits>
#include <iostream>
#include <map>
#include <random>

using namespace kvaaas;

TEST_CASE("SLBottomLevelRecordViewer tests") {
  FileByteArray arr("SkipListFile", true);
  SLBottomLevelRecordViewer viewer(&arr);
  CHECK(viewer.get_head() == NULL_NODE);
  viewer.set_head(100);
  CHECK(viewer.get_head() == 100);
  SLBottomLevelRecord record;
  record.next = 100;
  record.offset = 100;
  for (ByteType &byte : record.key) {
    byte = static_cast<ByteType>('a');
  }
  viewer.append_record(record);
  CHECK(record == viewer.get_record(0));

  static constexpr std::size_t SIZE = 100;
  std::vector<SLBottomLevelRecord> records(SIZE);
  std::random_device rd;
  std::mt19937_64 rnd(rd());
  for (std::size_t i = 0; i < SIZE; ++i) {
    records[i].next = rnd();
    records[i].offset = rnd();
    for (ByteType &j : records[i].key) {
      j = static_cast<ByteType>(rnd() % (UCHAR_MAX + 1));
    }
  }
  for (const auto &record : records) {
    viewer.append_record(record);
  }
  for (std::size_t i = 0; i < SIZE; ++i) {
    CHECK(viewer.get_record(i + 1) == records[i]);
  }

  CHECK(viewer.get_head() == 100);
}

TEST_CASE("SLUpperLevelRecordViewer tests") {
  FileByteArray arr("skipListFile", true);
  FileByteArray heads_arr("HeadsFile", true);
  SLUpperLevelRecordViewer viewer(&arr, &heads_arr);
  static constexpr std::size_t SIZE = 100;
  std::vector<uint64_t> heads(SIZE);
  std::random_device rd;
  std::mt19937_64 rnd(rd());
  for (std::size_t i = 0; i < SIZE; ++i) {
    heads[i] = rnd();
    viewer.append_head(heads[i]);
  }

  for (std::size_t i = 0; i < SIZE; ++i) {
    CHECK(heads[i] == viewer.get_head(i));
  }

  SLUpperLevelRecord record;
  record.next = 100;
  record.down = 100;
  for (ByteType &byte : record.key) {
    byte = static_cast<ByteType>('a');
  }
  viewer.append_record(record);
  CHECK(record == viewer[0]);

  std::vector<SLUpperLevelRecord> records(SIZE);
  for (std::size_t i = 0; i < SIZE; ++i) {
    records[i].next = rnd();
    records[i].down = rnd();
    for (ByteType &j : records[i].key) {
      j = static_cast<ByteType>(rnd() % (UCHAR_MAX + 1));
    }
  }
  for (const auto &record : records) {
    viewer.append_record(record);
  }
  for (std::size_t i = 0; i < SIZE; ++i) {
    CHECK(viewer[i + 1] == records[i]);
  }

  for (std::size_t i = 0; i < SIZE; ++i) {
    heads[i] = rnd();
    viewer.set_head(i, heads[i]);
  }
  for (std::size_t i = 0; i < SIZE; ++i) {
    CHECK(heads[i] == viewer.get_head(i));
  }
}

namespace {

ByteType gen_byte() {
  static std::random_device rnd_device;
  static std::mt19937 mersenne_engine{
      rnd_device()}; // Generates random integers
  static std::uniform_int_distribution<unsigned> dist{
      1, static_cast<unsigned>(std::numeric_limits<std::byte>::max())};
  return std::byte(dist(mersenne_engine));
}

KeyType gen_key() {
  KeyType key;
  for (auto &i : key) {
    i = gen_byte();
  }
  return key;
}

std::uint64_t gen_offset() {
  static std::random_device rnd_device;
  static std::mt19937_64 mersenne_engine{
      rnd_device()}; // Generates random integers
  static std::uniform_int_distribution<std::uint64_t> dist{
      0, std::numeric_limits<std::uint64_t>::max()};
  return dist(mersenne_engine);
}

} // namespace

TEST_CASE("SkipList tests simple") {
  RAMByteArray bottom;
  RAMByteArray heads;
  RAMByteArray upper;
  SLBottomLevelRecordViewer bottom_viewer(&bottom);
  SLUpperLevelRecordViewer upper_viewer(&upper, &heads);
  SkipList skip_list(bottom_viewer, upper_viewer);
  auto one = std::byte(1);
  auto two = std::byte(2);
  auto three = std::byte(3);
  auto four = std::byte(4);
  KeyType key1{one, two, three, four};
  skip_list.put(key1, 123);

  auto val = skip_list.find(key1);
  CHECK(val.has_value());
  CHECK(val.value() == 123);

  KeyType key2{one, one, one, one};
  skip_list.put(key2, 345);
  val = skip_list.find(key1);
  CHECK(val.has_value());
  CHECK(val.value() == 123);

  val = skip_list.find(key2);
  CHECK(val.has_value());
  CHECK(val.value() == 345);

  KeyType key3{one, one, three, four};
  skip_list.put(key3, 567);

  val = skip_list.find(key1);
  CHECK(val.has_value());
  CHECK(val.value() == 123);

  val = skip_list.find(key2);
  CHECK(val.has_value());
  CHECK(val.value() == 345);

  val = skip_list.find(key3);
  CHECK(val.has_value());
  CHECK(val.value() == 567);
}

TEST_CASE("SkipList stress-tests") {
  RAMByteArray bottom;
  RAMByteArray heads;
  RAMByteArray upper;
  SLBottomLevelRecordViewer bottom_viewer(&bottom);
  SLUpperLevelRecordViewer upper_viewer(&upper, &heads);
  SkipList skip_list(bottom_viewer, upper_viewer);

  std::random_device rnd_device;
  std::mt19937 mersenne_engine{rnd_device()}; // Generates random integers
  std::uniform_int_distribution<unsigned> dist{
      1, static_cast<unsigned>(std::numeric_limits<std::byte>::max())};

  std::vector<std::pair<KeyType, std::uint64_t>> records;
  auto has_key = [&records](const KeyType &key) {
    return std::any_of(records.begin(), records.end(),
                       [&key](const std::pair<KeyType, std::uint64_t> &elem) {
                         return elem.first == key;
                       });
  };
  static constexpr std::size_t UWU = 100;
  for (std::size_t i = 0; i < UWU; ++i) {
    while (true) {
      auto key = gen_key();
      if (!has_key(key)) {
        records.emplace_back(key, gen_offset());
        break;
      }
    }
    skip_list.put(records.back().first, records.back().second);
    for (const auto &[key, offset] : records) {
      auto val = skip_list.find(key);
      CHECK(val.has_value());
      CHECK(val.value() == offset);
    }
  }
}

TEST_CASE("Multiple assignment SkipList") {
  RAMByteArray bottom;
  RAMByteArray heads;
  RAMByteArray upper;
  SLBottomLevelRecordViewer bottom_viewer(&bottom);
  SLUpperLevelRecordViewer upper_viewer(&upper, &heads);
  SkipList skip_list(bottom_viewer, upper_viewer);

  KeyType key1{std::byte(1)};
  KeyType key2{std::byte(2)};
  KeyType key3{std::byte(3)};
  KeyType key4{std::byte(4)};
  KeyType key5{std::byte(5)};
  KeyType key6{std::byte(6)};

  skip_list.put(key1, 123);
  CHECK(skip_list.find(key1).value() == 123);

  skip_list.put(key1, 345);
  CHECK(skip_list.find(key1).value() == 345);

  skip_list.put(key2, 567);
  CHECK(skip_list.find(key2).value() == 567);

  skip_list.put(key2, 345);
  CHECK(skip_list.find(key2).value() == 345);

  skip_list.put(key5, 123);
  CHECK(skip_list.find(key5).value() == 123);

  skip_list.put(key4, 123);
  CHECK(skip_list.find(key4).value() == 123);

  skip_list.put(key6, 123);
  CHECK(skip_list.find(key6).value() == 123);

  skip_list.put(key3, 789);
  CHECK(skip_list.find(key3).value() == 789);

  skip_list.put(key3, 123);
  CHECK(skip_list.find(key3).value() == 123);

  skip_list.put(key2, 100);
  CHECK(skip_list.find(key2).value() == 100);

  std::vector to_push({std::make_pair(key1, 1), std::make_pair(key2, 2),
                       std::make_pair(key3, 3)});
  skip_list.push_from(to_push.begin(), to_push.end());

  CHECK(skip_list.find(key1).value() == 1);
  CHECK(skip_list.find(key2).value() == 2);
  CHECK(skip_list.find(key3).value() == 3);
}

void put(std::map<KeyType, std::uint64_t> &map, SkipList &skip_list,
         const KeyType &key, std::uint64_t offset) {
  map[key] = offset;
  skip_list.put(key, offset);
}

void find(std::map<KeyType, std::uint64_t> &map, SkipList &skip_list,
          const KeyType &key) {
  CHECK(map.size() == skip_list.size());
  auto val = skip_list.find(key);
  bool contains_in_map = map.count(key) == 1;
  if (contains_in_map) {
    CHECK(val.has_value());
  } else {
    CHECK(!val.has_value());
  }
  if (contains_in_map) {
    CHECK(val.value() == map[key]);
  }
}

TEST_CASE("SkipList with map stress-tests") {
  static std::random_device rnd_device;
  static std::mt19937_64 mersenne_engine{
      rnd_device()}; // Generates random integers
  static std::uniform_int_distribution<std::uint64_t> dist{0, 3};

  RAMByteArray bottom;
  RAMByteArray heads;
  RAMByteArray upper;
  SLBottomLevelRecordViewer bottom_viewer(&bottom);
  SLUpperLevelRecordViewer upper_viewer(&upper, &heads);
  SkipList skip_list(bottom_viewer, upper_viewer);

  std::map<KeyType, std::uint64_t> map;

  std::vector<KeyType> used_keys;
  static constexpr std::size_t UWU1 = 200;

  auto put_new_key = [&]() {
    KeyType key = gen_key();
    std::uint64_t offset = gen_offset();
    put(map, skip_list, key, offset);
    used_keys.emplace_back(key);
  };

  auto put_old_key = [&]() {
    KeyType key = used_keys[mersenne_engine() % used_keys.size()];
    std::uint64_t offset = gen_offset();
    put(map, skip_list, key, offset);
  };

  auto call_find_old_key = [&] {
    KeyType key = used_keys[mersenne_engine() % used_keys.size()];
    find(map, skip_list, key);
  };

  auto call_find_new_key = [&] {
    KeyType key = gen_key();
    find(map, skip_list, key);
  };

  for (std::size_t i = 0; i < UWU1; ++i) {
    put_new_key();
  }

  static constexpr std::size_t UWU2 = 10000;

  for (std::size_t i = 0; i < UWU2; ++i) {
    switch (dist(mersenne_engine)) {
    case 0:
      put_new_key();
      break;
    case 1:
      put_old_key();
      break;
    case 2:
      call_find_old_key();
      break;
    case 3:
      call_find_new_key();
      break;
    }
  }
}

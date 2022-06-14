#include "Shard.h"

#include "doctest.h"

#include <array>
#include <vector>

namespace {
using namespace kvaaas;

ShardOption little_in_ram{
    true, ManagerType::RAMMM,
    2,    // log max size
    2,    // skip list max size
    2000, // sst max size
    0.5,
};

ShardOption big_in_ram{
    true,   ManagerType::RAMMM,
    1000,   // log max size
    5000,   // skip list max size
    100000, // sst max size
    0.5,
};
std::random_device rnd_device;
std::mt19937 mersenne_engine{rnd_device()}; // Generates random integers
std::uniform_int_distribution<unsigned> dist{
    1, static_cast<unsigned>(std::numeric_limits<std::byte>::max())};

auto gen_byte = []() { return std::byte(dist(mersenne_engine)); };
auto gen_key = [] {
  KeyType key;
  for (std::size_t i = 0; i < key.size(); ++i) {
    key[i] = gen_byte();
  }
  return key;
};

auto gen_value = [] {
  ValueType val(unsigned(gen_byte()) + 1);
  for (auto &bt : val) {
    bt = gen_byte();
  }
  return val;
};

TEST_CASE("Just Creates") { Shard shard("shard_test", little_in_ram); }

TEST_CASE("In-Log put/get") {
  Shard shard("shard_test", little_in_ram);
  const std::size_t N = 10;
  std::array<KeyType, N> keys;
  std::array<ValueType, N> values;
  for (std::size_t i = 0; i < N; ++i) {
    keys[i] = {std::byte(i)};
    values[i] = ValueType(1 + std::size_t(i), std::byte(i * i));
    shard.add(keys[i], values[i]);
  }

  for (std::size_t i = 0; i < N; ++i) {
    CHECK((*shard.get(keys[i])) == std::pair{keys[i], values[i]});
  }
}

TEST_CASE("MergeIntoSkipList") {
  Shard shard("shard_test", little_in_ram);
  const std::size_t N = 30;
  std::array<KeyType, N> keys{};
  std::array<ValueType, N> values;
  for (std::size_t i = 0; i < N; ++i) {
    keys[i] = {std::byte(i)};
    values[i] = ValueType(1 + std::size_t(i), std::byte(1 + i));
    shard.add(keys[i], values[i]);
  }

  for (std::size_t i = 0; i < N; ++i) {
    CHECK((*shard.get(keys[i])) == std::pair{keys[i], values[i]});
  }
}

TEST_CASE("MergeIntoSST") {
  Shard shard("shard_test", little_in_ram);
  const std::size_t N = 1011;
  std::array<KeyType, N> keys{};
  std::array<ValueType, N> values;
  for (std::size_t i = 0; i < N; ++i) {
    keys[i] = gen_key();
    values[i] = ValueType(100, gen_byte());
    shard.add(keys[i], values[i]);
  }

  for (std::size_t i = 0; i < N; ++i) {
    CHECK(shard.get(keys[i]));
    CHECK((*shard.get(keys[i])) == std::pair{keys[i], values[i]});
  }
}
TEST_CASE("Remove") {
  Shard shard("shard_test", little_in_ram);
  const std::size_t N = 1200;
  std::array<KeyType, N> keys{};
  std::array<ValueType, N> values;
  for (std::size_t i = 0; i < N; ++i) {
    keys[i] = gen_key();
    values[i] = ValueType(100, gen_byte());
    shard.add(keys[i], values[i]);
  }
  for (std::size_t i = 0; i < N; ++i) {
    if (i % 2) {
      shard.remove(keys[i]);
    }
  }

  for (std::size_t i = 0; i < N; ++i) {

    if (i % 2) {
      CHECK(!shard.get(keys[i]));
    } else {
      CHECK(shard.get(keys[i]));
      CHECK((*shard.get(keys[i])) == std::pair{keys[i], values[i]});
    }
  }
}

void put(std::map<KeyType, ValueType> &map, Shard &shard, const KeyType &key,
         const ValueType &value) {
  map[key] = value;
  shard.add(key, value);
}

void find(std::map<KeyType, ValueType> &map, Shard &shard, const KeyType &key) {
  auto val = shard.get(key);
  bool contains_in_map = map.count(key) == 1;
  CHECK(val.has_value() == contains_in_map);
  if (contains_in_map) {
    const auto &shard_vec = val.value().second;
    const auto &map_vec = map[key];
    CHECK(shard_vec == map_vec);
  }
}

TEST_CASE("Add same-key queries") {
  Shard shard("shard_test", little_in_ram);
  auto one = std::byte(1);
  auto two = std::byte(2);
  auto three = std::byte(3);
  auto four = std::byte(4);
  auto five = std::byte(5);
  KeyType key1{one, two};
  KeyType key2{two, three};
  KeyType key3{four};

  shard.add(key1, std::vector{two, three});
  CHECK(shard.get(key1).has_value());
  CHECK(shard.get(key1).value().second == std::vector{two, three});

  shard.add(key2, std::vector{two});
  CHECK(shard.get(key2).has_value());
  CHECK(shard.get(key2).value().second == std::vector{two});

  shard.add(key3, std::vector{five});
  CHECK(shard.get(key3).has_value());
  CHECK(shard.get(key3).value().second == std::vector{five});

  CHECK(shard.get(key1).has_value());
  CHECK(shard.get(key1).value().second == std::vector{two, three});

  shard.add(key1, std::vector{three, four});
  CHECK(shard.get(key1).has_value());
  CHECK(shard.get(key1).value().second == std::vector{three, four});

  shard.add(key1, std::vector{four});
  CHECK(shard.get(key1).has_value());
  CHECK(shard.get(key1).value().second == std::vector{four});

  shard.add(key2, std::vector{one});
  CHECK(shard.get(key2).has_value());
  CHECK(shard.get(key2).value().second == std::vector{one});

  shard.add(key3, std::vector{one});
  CHECK(shard.get(key3).has_value());
  CHECK(shard.get(key3).value().second == std::vector{one});
}

TEST_CASE("Shard with map stress-tests") {
  static std::random_device rnd_device;
  static std::mt19937_64 mersenne_engine{
      rnd_device()}; // Generates random integers
  static std::uniform_int_distribution<std::uint64_t> dist{0, 3};

  Shard shard("shard_test", big_in_ram);

  std::map<KeyType, ValueType> map;

  std::vector<KeyType> used_keys;
  static constexpr std::size_t UWU1 = 200;

  auto put_new_key = [&]() {
    KeyType key = gen_key();
    put(map, shard, key, gen_value());
    used_keys.emplace_back(key);
  };

  auto put_old_key = [&]() {
    KeyType key = used_keys[mersenne_engine() % used_keys.size()];
    put(map, shard, key, gen_value());
  };

  auto call_find_old_key = [&] {
    KeyType key = used_keys[mersenne_engine() % used_keys.size()];
    find(map, shard, key);
  };

  auto call_find_new_key = [&] {
    KeyType key = gen_key();
    find(map, shard, key);
  };

  for (std::size_t i = 0; i < UWU1; ++i) {
    put_new_key();
  }

  static constexpr std::size_t UWU2 = 1000;

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

} // namespace

#include "Kvaaas.h"

#include "doctest.h"

#include <array>
#include <vector>

namespace {
using namespace kvaaas;

KvaaasOption little_in_ram_kvaaas{
    true, ManagerType::RAMMM,
    2,    // log max size
    2,    // skip list max size
    2000, // sst max size
    0.5,
    3 // kvaaas_cnt
};

KvaaasOption big_in_ram_kvaaas{
    true,  ManagerType::RAMMM,
    1000,  // log max size
    5000,  // skip list max size
    10000, // sst max size
    0.5,
    3 // kvaaas_cnt
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

TEST_CASE("Just Creates") {
  Kvaaas kvaaas("kvaaas_test", little_in_ram_kvaaas);
}

TEST_CASE("In-Log put/get") {
  Kvaaas kvaaas("kvaaas_test", little_in_ram_kvaaas);
  const std::size_t N = 10;
  std::array<KeyType, N> keys;
  std::array<ValueType, N> values;
  for (std::size_t i = 0; i < N; ++i) {
    keys[i] = {std::byte(i)};
    values[i] = ValueType(1 + std::size_t(i), std::byte(i * i));
    kvaaas.add(keys[i], values[i]);
  }

  for (std::size_t i = 0; i < N; ++i) {
    CHECK((*kvaaas.get(keys[i])) == std::pair{keys[i], values[i]});
  }
}

TEST_CASE("MergeIntoSkipList") {
  Kvaaas kvaaas("kvaaas_test", little_in_ram_kvaaas);
  const std::size_t N = 30;
  std::array<KeyType, N> keys{};
  std::array<ValueType, N> values;
  for (std::size_t i = 0; i < N; ++i) {
    keys[i] = {std::byte(i)};
    values[i] = ValueType(1 + std::size_t(i), std::byte(1 + i));
    kvaaas.add(keys[i], values[i]);
  }

  for (std::size_t i = 0; i < N; ++i) {
    CHECK((*kvaaas.get(keys[i])) == std::pair{keys[i], values[i]});
  }
}

TEST_CASE("MergeIntoSST") {
  Kvaaas kvaaas("kvaaas_test", little_in_ram_kvaaas);
  const std::size_t N = 1011;
  std::array<KeyType, N> keys{};
  std::array<ValueType, N> values;
  for (std::size_t i = 0; i < N; ++i) {
    keys[i] = gen_key();
    values[i] = ValueType(100, gen_byte());
    kvaaas.add(keys[i], values[i]);
  }

  for (std::size_t i = 0; i < N; ++i) {
    CHECK(kvaaas.get(keys[i]));
    CHECK((*kvaaas.get(keys[i])) == std::pair{keys[i], values[i]});
  }
}
TEST_CASE("Remove") {
  Kvaaas kvaaas("kvaaas_test", little_in_ram_kvaaas);
  const std::size_t N = 1200;
  std::array<KeyType, N> keys{};
  std::array<ValueType, N> values;
  for (std::size_t i = 0; i < N; ++i) {
    keys[i] = gen_key();
    values[i] = ValueType(100, gen_byte());
    kvaaas.add(keys[i], values[i]);
  }
  for (std::size_t i = 0; i < N; ++i) {
    if (i % 2) {
      kvaaas.remove(keys[i]);
    }
  }

  for (std::size_t i = 0; i < N; ++i) {

    if (i % 2) {
      CHECK(!kvaaas.get(keys[i]));
    } else {
      CHECK(kvaaas.get(keys[i]));
      CHECK((*kvaaas.get(keys[i])) == std::pair{keys[i], values[i]});
    }
  }
}

void put(std::map<KeyType, ValueType> &map, Kvaaas &kvaaas, const KeyType &key,
         const ValueType &value) {
  map[key] = value;
  kvaaas.add(key, value);
}

void find(std::map<KeyType, ValueType> &map, Kvaaas &kvaaas,
          const KeyType &key) {
  auto val = kvaaas.get(key);
  bool contains_in_map = map.count(key) == 1;
  CHECK(val.has_value() == contains_in_map);
  if (contains_in_map) {
    const auto &kvaaas_vec = val.value().second;
    const auto &map_vec = map[key];
    CHECK(kvaaas_vec == map_vec);
  }
}

TEST_CASE("Add same-key queries") {
  Kvaaas kvaaas("kvaaas_test", little_in_ram_kvaaas);
  auto one = std::byte(1);
  auto two = std::byte(2);
  auto three = std::byte(3);
  auto four = std::byte(4);
  auto five = std::byte(5);
  KeyType key1{one, two};
  KeyType key2{two, three};
  KeyType key3{four};

  kvaaas.add(key1, std::vector{two, three});
  CHECK(kvaaas.get(key1).has_value());
  CHECK(kvaaas.get(key1).value().second == std::vector{two, three});

  kvaaas.add(key2, std::vector{two});
  CHECK(kvaaas.get(key2).has_value());
  CHECK(kvaaas.get(key2).value().second == std::vector{two});

  kvaaas.add(key3, std::vector{five});
  CHECK(kvaaas.get(key3).has_value());
  CHECK(kvaaas.get(key3).value().second == std::vector{five});

  CHECK(kvaaas.get(key1).has_value());
  CHECK(kvaaas.get(key1).value().second == std::vector{two, three});

  kvaaas.add(key1, std::vector{three, four});
  CHECK(kvaaas.get(key1).has_value());
  CHECK(kvaaas.get(key1).value().second == std::vector{three, four});

  kvaaas.add(key1, std::vector{four});
  CHECK(kvaaas.get(key1).has_value());
  CHECK(kvaaas.get(key1).value().second == std::vector{four});

  kvaaas.add(key2, std::vector{one});
  CHECK(kvaaas.get(key2).has_value());
  CHECK(kvaaas.get(key2).value().second == std::vector{one});

  kvaaas.add(key3, std::vector{one});
  CHECK(kvaaas.get(key3).has_value());
  CHECK(kvaaas.get(key3).value().second == std::vector{one});
}

TEST_CASE("Kvaaas with map stress-tests") {
  static std::random_device rnd_device;
  static std::mt19937_64 mersenne_engine{
      rnd_device()}; // Generates random integers
  static std::uniform_int_distribution<std::uint64_t> dist{0, 3};

  Kvaaas kvaaas("kvaaas_test", big_in_ram_kvaaas);

  std::map<KeyType, ValueType> map;

  std::vector<KeyType> used_keys;
  static constexpr std::size_t UWU1 = 200;

  auto put_new_key = [&]() {
    KeyType key = gen_key();
    put(map, kvaaas, key, gen_value());
    used_keys.emplace_back(key);
  };

  auto put_old_key = [&]() {
    KeyType key = used_keys[mersenne_engine() % used_keys.size()];
    put(map, kvaaas, key, gen_value());
  };

  auto call_find_old_key = [&] {
    KeyType key = used_keys[mersenne_engine() % used_keys.size()];
    find(map, kvaaas, key);
  };

  auto call_find_new_key = [&] {
    KeyType key = gen_key();
    find(map, kvaaas, key);
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

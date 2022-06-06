#include "Shard.h"

#include "doctest.h"

#include <array>
#include <vector>

namespace {
using namespace kvaaas;

ShardOption little_in_ram{
    true, ManagerType::RAMMM,
    20,   // log max size
    200,  // skip list max size
    2000, // sst max size
    3.,
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
} // namespace

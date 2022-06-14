#include "Kvaaas.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <set>
#include <fstream> 
#include "json.hpp"

namespace {
using namespace kvaaas;

std::random_device rnd_device;
std::mt19937 mersenne_engine{rnd_device()}; 
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
  ValueType val(2048u);
  for (auto &bt : val) {
    bt = gen_byte();
  }
  return val;
};

using Nanoseconds = std::chrono::duration<long long, std::nano>;
using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
inline TimePoint Now() { return Clock::now(); }

struct stat {
  Nanoseconds time_per_request;
  Nanoseconds time_per_write;
  Nanoseconds time_per_read;
  Nanoseconds max_per_read;
  Nanoseconds max_per_write;
};

stat collect_stat(const unsigned already, const unsigned total,
                  const unsigned read_percent) {
  // Write "already in" recordings
  std::set<KeyType> keys_already_in;
  while (keys_already_in.size() < already) {
    keys_already_in.emplace(gen_key());
  }

  std::filesystem::create_directory("abobus");
  Kvaaas kvs("abobus", DefaultOnDisk);

  for (const auto &key : keys_already_in) {
    kvs.add(key, gen_value());
  }

  std::discrete_distribution disc_dist(
      {1.0 * read_percent, 100. - read_percent}); // 0 - reading
  Nanoseconds sum_read{};
  Nanoseconds sum_write{};
  std::size_t cnt_read{};
  std::size_t cnt_write{};
  Nanoseconds max_per_write = Nanoseconds::min();
  Nanoseconds max_per_read = Nanoseconds::min();
  for (std::size_t i = 0; i < total; ++i) {
    int type = disc_dist(mersenne_engine);
    KeyType key = i % 2 == 0? *std::next(keys_already_in.begin(), mersenne_engine() % already) : gen_key();
    if (type == 0) { // reading
      ++cnt_read;
      TimePoint begin = Now();
      kvs.get(key);
      TimePoint end = Now();
      sum_read += end - begin;
      max_per_read = std::max<Nanoseconds>(max_per_read, end - begin);
    } else if (type == 1) { // writing
      ++cnt_write;
      TimePoint begin = Now();
      kvs.add(key, gen_value());
      TimePoint end = Now();
      sum_write += end - begin;
      max_per_write = std::max<Nanoseconds>(max_per_write, end - begin);
    }
  }
  return {(sum_read + sum_write) / total, sum_write / cnt_write,
          sum_read / cnt_read, max_per_read, max_per_write};
}

} // namespace

// usage: %program% ALREADY_IN TOTAL_QUERIES READ_PERCENT OUTPUT_FILE
int main(const int argc, const char **argv) {
  if (argc < 5) {
    std::cout << "Usage: %program% ALREADY_IN TOTAL_QUERIES READ_PERCENT"
              << std::endl;
    return 1;
  }
  const unsigned ALREADY_IN = [argv]() {
    unsigned t = 1;
    sscanf(argv[1], "%u", &t);
    return t;
  }();
  const unsigned TOTAL_QUERIES = [argv]() {
    unsigned t = 1;
    sscanf(argv[2], "%u", &t);
    return t;
  }();
  const unsigned READ_PERCENT = [argv]() {
    unsigned t = 1;
    sscanf(argv[3], "%u", &t);
    assert(t <= 100);
    return t;
  }();

  auto stat = collect_stat(ALREADY_IN, TOTAL_QUERIES, READ_PERCENT);

  std::ofstream out(argv[4]);
  nlohmann::json json;
  json["already_in"] = ALREADY_IN;
  json["total_queries"] = TOTAL_QUERIES;
  json["read_percent"] = READ_PERCENT;
  json["avg_per_request"] = stat.time_per_request.count();
  json["avg_per_write"] = stat.time_per_write.count();
  json["max_per_write"] = stat.max_per_write.count();
  json["avg_per_read"] = stat.time_per_read.count();
  json["max_per_read"] = stat.max_per_read.count();
  out << json;
}

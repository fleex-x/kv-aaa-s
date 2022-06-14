#pragma once

#include "Core.h"
#include "xxhash.h"
#include <algorithm>
#include <random>
#include <vector>

namespace kvaaas {

struct defer {};
class BloomFilter {
public:
  explicit BloomFilter(std::size_t elements_cnt)
      : _function_cnt(std::max(std::size_t(3), lg(elements_cnt) + 1)),
        _seeds(_function_cnt),
        _data(std::max(std::size_t(10), elements_cnt * C)) {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0,
                                                                  _data.size());
    for (size_t i = 0; i < _seeds.size(); ++i) {
      _seeds[i] = dist(rng);
    }
  }

  bool has_key(const KeyType &key) {
    bool has = true;
    for (std::size_t i = 0; i < _function_cnt; ++i) {
      has = has && _data[hashAt(key, i)];
    }
    return has;
  }

  void add(const KeyType &key) {
    for (std::size_t i = 0; i < _function_cnt; ++i) {
      _data[hashAt(key, i)] = true;
    }
  }

private:
  static constexpr std::size_t C = 6;
  std::size_t lg(std::size_t x) {
    std::size_t res = 0;
    while (x > (std::size_t(1) << res)) {
      ++res;
    }
    return res;
  }
  uint32_t hashAt(const KeyType &key, std::size_t index) const noexcept {
    uint32_t current_seed = _seeds[index];
    return XXH32(key.data(), key.size(), current_seed) % _data.size();
  }
  const std::size_t _function_cnt;
  std::vector<uint32_t> _seeds;
  std::vector<bool> _data;
};
} // namespace kvaaas

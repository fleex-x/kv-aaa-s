#pragma once

#include "Core.h"
#include "xxhash.h"
#include <random>
#include <vector>

namespace kvaaas {

struct defer {};
class BloomFilter {
public:
  explicit BloomFilter(std::size_t elements_cnt, std::size_t function_cnt = 5)
      : _function_cnt(function_cnt), _seeds(function_cnt),
        _data(elements_cnt + 1) {
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
  uint32_t hashAt(const KeyType &key, std::size_t index) const noexcept {
    uint32_t current_seed = _seeds[index];
    return XXH32(key.data(), key.size(), current_seed) % _data.size();
  }
  const std::size_t _function_cnt;
  std::vector<uint32_t> _seeds;
  std::vector<bool> _data;
};
} // namespace kvaaas

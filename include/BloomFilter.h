#pragma once

#include "xxhash.h"
#include "Core.h"
#include <random>
#include <vector>

namespace kvaaas {
class BloomFilter {
public:
  explicit BloomFilter(std::size_t elements_cnt, std::size_t function_cnt = 5)
      : _seeds(function_cnt), _data(elements_cnt) {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0,
                                                                  _data.size());
    for (size_t i = 0; i < _seeds.size(); ++i) {
      _seeds[i] = dist(rng);
    }
  }

private:
	uint32_t hashAt(const KeyType & key, std::size_t index) const noexcept {
		uint32_t current_seed = _seeds[index];
		return XXH32(key.data(), key.size(), current_seed);
	}


  std::vector<uint32_t> _seeds;
  std::vector<bool> _data;
};
} // namespace kvaaas

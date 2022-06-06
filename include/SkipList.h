#pragma once
#include "Core.h"
#include "SST.h"
#include "SkipListRecords.h"
#include <optional>
#include <random>

namespace kvaaas {

class SkipList {

private:
  SLBottomLevelRecordViewer bottom;
  SLUpperLevelRecordViewer upper;
  std::size_t levels_count;
  std::uint64_t insert_after_on_upper_level(std::uint64_t ind,
                                            SLUpperLevelRecord &new_node);
  std::uint64_t insert_as_head_upper_level(std::uint64_t level,
                                           SLUpperLevelRecord &new_node);
  std::uint64_t insert_after_on_bottom_level(std::uint64_t ind,
                                             SLBottomLevelRecord &new_node);
  std::uint64_t insert_as_head_bottom_level(SLBottomLevelRecord &new_node);

  void insert_after_parents(const std::vector<std::uint64_t> &parents,
                            const KeyType &key, std::uint64_t offset);

  std::uint64_t insert(std::uint64_t level, std::uint64_t after_pos,
                       std::uint64_t down, std::uint64_t offset,
                       const KeyType &key);

  std::random_device rd;
  std::mt19937 rng{0};
  std::uniform_int_distribution<std::mt19937::result_type> dist{0, 1};

public:
  SkipList(const SLBottomLevelRecordViewer &bottom_,
           const SLUpperLevelRecordViewer &upper_);
  void put(const KeyType &key, std::uint64_t offset);
  std::optional<std::uint64_t> find(const KeyType &key);
  bool has_key(const KeyType &key);

  template <typename It> void push_from(It begin, It end) {
    for (It it = begin; it != end; ++it) {
      std::cerr << "New put" << std::endl;
      put(it->first, it->second);
    }
  }

  class iterator {
  private:
    SkipList *owner = nullptr;
    std::uint64_t cur_node = NULL_NODE;

    iterator(SkipList *owner_, std::uint64_t cur_node_)
        : owner(owner_), cur_node(cur_node_) {}
    friend SkipList;

  public:
    using value_type = SSTRecord;

    const value_type operator*() {
      auto record = owner->bottom.get_record(cur_node);
      return {record.key, record.offset};
    }

    iterator &operator++() {
      cur_node = owner->bottom.get_next(cur_node);
      return *this;
    }

    iterator operator++(int) {
      iterator iterator = *this;
      ++(*this);
      return iterator;
    }

    bool operator==(const iterator &oth) {
      return owner == oth.owner && cur_node == oth.cur_node;
    }

    bool operator!=(const iterator &oth) { return !(*this == oth); }
  };

  iterator begin() { return {this, bottom.get_head()}; }

  iterator end() { return {this, NULL_NODE}; }

  friend iterator;

  std::uint64_t size() const;
};

} // namespace kvaaas

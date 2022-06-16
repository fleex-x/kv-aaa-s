#pragma once

#include "BloomFilter.h"
#include "ByteArray.h"
#include "Core.h"

#include <algorithm>
#include <cstring>
#include <iostream> // for debug, remove later
#include <vector>

namespace kvaaas {

struct SSTRecord {
  KeyType key{};
  std::uint64_t offset;
  bool operator<(SSTRecord oth) const { return key < oth.key; }
  bool operator==(SSTRecord oth) const { return key == oth.key; }
  bool operator!=(SSTRecord oth) const { return key != oth.key; }
};

struct NewSSTRV {};
struct RebuildSSTRV {};

// It just take an underlaying bytearrat
// If wanna EMPTY one, than inject an empty byte array!
struct SSTRecordViewer {
  SSTRecordViewer(ByteArrayPtr data, NewSSTRV)
      : _data(data) {} // remove later ??

  void append(const SSTRecord &rec) {
      _data->append(rec.key.data(), rec.key.size());
      _data->append(reinterpret_cast<const ByteType *>(&rec.offset), sizeof(rec.offset));
  }

  SSTRecord get_record(std::size_t index) {
    SSTRecord rec;
    static const std::size_t REC_SIZE = KEY_SIZE_BYTES + sizeof(std::uint64_t);
    auto begin = index * REC_SIZE;
    _data->read_ptr(rec.key.data(), begin, begin + rec.key.size());
    _data->read_ptr(reinterpret_cast<ByteType *>(&rec.offset), begin + rec.key.size(), begin + rec.key.size() + sizeof(rec.offset));
    return rec;
  }

  void change_offset(std::size_t index, std::uint64_t new_offset) {
      static const std::size_t REC_SIZE = KEY_SIZE_BYTES + sizeof(std::uint64_t);
      _data->rewrite(REC_SIZE * index + KEY_SIZE_BYTES, reinterpret_cast<const ByteType *>(&new_offset), sizeof(new_offset));
  }

  SSTRecordViewer(ByteArrayPtr data, RebuildSSTRV) : _data(data) {}

  std::uint64_t size() const noexcept {
    return _data->size() / (sizeof(std::uint64_t) + KEY_SIZE_BYTES);
  }

  bool same_layout(const SSTRecordViewer &oth) { return _data == oth._data; }

private:
  ByteArrayPtr _data;
};

struct SST {

  explicit SST(SSTRecordViewer rec_viewer)
      : _rec_view(std::move(rec_viewer)), bf(_rec_view.size() + 10) {
    for (std::size_t i = 0; i < _rec_view.size(); ++i) {
      bf.add(rec_viewer.get_record(i).key);
    }
  }

  struct iterator {
    using value_type = SSTRecord;

    iterator(SSTRecordViewer view, std::uint64_t index)
        : _view(view), _index(index) {}

    const value_type operator*() { return _view.get_record(_index); }

    iterator &operator++() {
      _index++;
      return *this;
    }

    iterator operator++(int) {
      iterator iterator = *this;
      ++(*this);
      return iterator;
    }

    bool operator==(const iterator &oth) {
      return _view.same_layout(oth._view) && _index == oth._index;
    }

    bool operator!=(const iterator &oth) { return !(*this == oth); }

  private:
    SSTRecordViewer _view;
    std::uint64_t _index;
  };

  iterator begin() { return iterator(_rec_view, 0); }

  iterator end() { return iterator(_rec_view, _rec_view.size()); }

  std::uint64_t size() const noexcept { return _rec_view.size(); }

  // TODO put bin search in private method or use std::binary_search

  bool contains(const KeyType &key) {
    if (!bf.has_key(key)) {
      return false;
    }
    std::int64_t left = 0;                 // less or equal
    std::int64_t right = _rec_view.size(); // not valid

    while (left + 1 < right) {
      auto mid = left + (right - left) / 2;
      auto rec = _rec_view.get_record(mid);
      if (rec.key <= key) {
        left = mid;
      } else {
        right = mid;
      }
    }
    auto rec = _rec_view.get_record(left);

    return rec.key == key;
  }

  std::uint64_t find_offset(const KeyType &key) {
    // find record
    std::int64_t left = 0;                 // less or equal
    std::int64_t right = _rec_view.size(); // not valid

    while (left + 1 < right) {
      auto mid = left + (right - left) / 2;
      auto rec = _rec_view.get_record(mid);
      if (rec.key <= key) {
        left = mid;
      } else {
        right = mid;
      }
    }
    auto rec = _rec_view.get_record(left);
    if (rec.key != key) {
    }
    return rec.offset;
  }

  template <typename It1, typename It2>
  static SST merge_into_sst(It1 begin1, It1 end1, It2 begin2, It2 end2,
                            SSTRecordViewer viewer) {
    if (viewer.size() != 0) {
    }

    while (begin1 != end1 && begin2 != end2) {
      const SSTRecord first = *begin1;
      const SSTRecord second = *begin2;
      if (first < second) {
        viewer.append(first);
        ++begin1;
      } else if (second < first) {
        viewer.append(second);
        ++begin2;
      } else {
        viewer.append(first);
        ++begin1;
        ++begin2;
      }
    }
    while (begin1 != end1) {
      viewer.append(*begin1++);
    }
    while (begin2 != end2) {
      viewer.append(*begin2++);
    }
    return SST(viewer);
  }

  void change_offset(const KeyType &key, std::uint64_t new_offset) {
    std::int64_t left = 0;                 // less or equal
    std::int64_t right = _rec_view.size(); // not valid

    while (left + 1 < right) {
      auto mid = left + (right - left) / 2;
      auto rec = _rec_view.get_record(mid);
      if (rec.key <= key) {
        left = mid;
      } else {
        right = mid;
      }
    }
    _rec_view.change_offset(left, new_offset);
  }

  void change_offset(std::size_t idx, std::uint64_t new_offset) {
    _rec_view.change_offset(idx, new_offset);
  }

private:
  SSTRecordViewer _rec_view;
  BloomFilter bf;
};

} // namespace kvaaas

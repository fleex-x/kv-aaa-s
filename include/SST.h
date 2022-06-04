#pragma once

#include "ByteArray.h"
#include "Core.h"
#include <algorithm>
#include <vector>

#include <iostream> // for debug, remove later

namespace kvaaas {

struct SSTRecord {
  KeyType key{};
  std::uint64_t offset;
  bool operator<(SSTRecord oth) const {
    return key < oth.key;
  }
};

struct NewSSTRV {};
struct RebuildSSTRV {};

struct SSTRecordViewer {
  SSTRecordViewer(ByteArrayPtr data, NewSSTRV) : _data(data) {}
  void append(SSTRecord rec) {
    std::vector<ByteType> vec(rec.key.size() + sizeof(std::uint64_t));
    std::copy(rec.key.begin(), rec.key.end(), vec.begin());
    std::memcpy(vec.data() + rec.key.size(), &rec.offset,
                sizeof(std::uint64_t));
    _data->append(vec);
  }

  SSTRecord get_record(std::size_t index) {
    SSTRecord rec;
    const std::size_t REC_SIZE = rec.key.size() + sizeof(std::uint64_t);
    auto vec = _data->read(index * REC_SIZE, (index + 1) * REC_SIZE);
    std::copy(vec.begin(), std::next(vec.begin(), rec.key.size()),
              rec.key.begin());
    std::memcpy(&rec.offset, vec.data() + rec.key.size(),
                sizeof(std::uint64_t));
    return rec;
  }

  // TODO(mkornaukhov03)
  // support rebuild from byte array
  SSTRecordViewer(ByteArrayPtr data, RebuildSSTRV) : _data(data) {}
  void store_size() {
    // write _size pn disk
  }

  void reset_size(std::size_t new_size) { _size = new_size; }
  std::uint64_t size() { return _size; }

private:
  std::uint64_t _size;
  ByteArrayPtr _data;
};

struct SST {

  explicit SST(SSTRecordViewer rec_viewer) : _rec_view(rec_viewer) {}

  struct Iterator {
    using value_type = SSTRecord;

    Iterator(SSTRecordViewer view, std::uint64_t index)
        : _view(view), _index(index) {}

    const value_type operator*() {
      return _view.get_record(_index);
    }

    Iterator& operator++() {
      _index++;
      return *this;
    }

    Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

  private:
    SSTRecordViewer _view;
    std::uint64_t _index;
  };

  Iterator begin() {
    return Iterator(_rec_view, 0);
  }

  Iterator end() {
    return Iterator(_rec_view, _rec_view.size());
  }

  size_t find_offset(const KeyType &key) {
    // find record
    std::int64_t left = 0;                 // less or equal
    std::int64_t right = _rec_view.size(); // not valid

    while (left + 1 < right) {
      auto mid = left + (right - left) / 2;
      auto rec = _rec_view.get_record(mid);
      if (rec.key == key) {
        left = mid;
      } else {
        right = mid;
      }
    }
    auto rec = _rec_view.get_record(left);
    if (rec.key != key) {
      std::cerr << "record key != key" << std::endl;
    }
    return rec.offset;
  }

  template<typename It1, typename It2>
  static SST merge_into_sst(It1 begin1, It1 end1, It2 begin2, It2 end2, SSTRecordViewer viewer) {
    if (viewer.size() != 0) {
      std::cerr << "Assume viewer size for merge = 0!" << std::endl;
    }
    while (begin1 != end1 && begin2 != end2) {
      const SSTRecord first = *begin1;
      const SSTRecord second = *begin2;
      if (first < second) {
        viewer.append(first);
        ++begin1;
      }
      else {
        viewer.append(second);
        ++begin2;
      }
    } 
  }

private:
  SSTRecordViewer _rec_view;
};

} // namespace kvaaas

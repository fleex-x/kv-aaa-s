#include "SkipListRecords.h"
#include "ByteArray.h"
#include "Core.h"
#include <cstring>
#include <iostream>
#include <vector>

namespace kvaaas {

bool operator==(const SLBottomLevelRecord &record1,
                const SLBottomLevelRecord &record2) {
  return record1.next == record2.next && record1.offset == record2.offset &&
         record1.key == record2.key;
}

SLBottomLevelRecord::SLBottomLevelRecord(const KeyType &key_,
                                         std::uint64_t offset_)
    : offset(offset_), key(key_) {}

SLBottomLevelRecordViewer::SLBottomLevelRecordViewer(ByteArrayPtr byte_arr_)
    : byte_arr(byte_arr_) {
  byte_arr->append(std::vector<ByteType>(HEAD_SIZE));
  set_head(NULL_NODE);
}

std::uint64_t SLBottomLevelRecordViewer::get_begin(std::uint64_t ind) {
  return HEAD_SIZE + ind * SLBottomLevelRecord::SIZE;
}

bool SLBottomLevelRecordViewer::has_head() { return get_head() != NULL_NODE; }

std::uint64_t SLBottomLevelRecordViewer::get_head() {
  std::vector<ByteType> head_chars = byte_arr->read(0, HEAD_SIZE);
  std::uint64_t head = 0;
  std::memcpy(&head, head_chars.data(), HEAD_SIZE);
  return head;
}

void SLBottomLevelRecordViewer::set_head(std::uint64_t head) {
  std::vector<ByteType> head_chars(HEAD_SIZE);
  std::memcpy(head_chars.data(), &head, HEAD_SIZE);
  byte_arr->rewrite(0, head_chars);
}

SLBottomLevelRecord SLBottomLevelRecordViewer::get_record(std::uint64_t ind) {
  std::vector<ByteType> record_chars =
      byte_arr->read(get_begin(ind), get_begin(ind + 1));
  SLBottomLevelRecord record;
  std::memcpy(&record.next,
              record_chars.data() + SLBottomLevelRecord::NEXT_BEGIN,
              sizeof(record.next));
  std::memcpy(&record.offset,
              record_chars.data() + SLBottomLevelRecord::OFFSET_BEGIN,
              sizeof(record.offset));
  std::memcpy(record.key.data(),
              record_chars.data() + SLBottomLevelRecord::KEY_BEGIN,
              KEY_SIZE_BYTES);
  return record;
}

std::uint64_t SLBottomLevelRecordViewer::get_next(std::uint64_t ind) {
  std::vector<ByteType> next_chars =
      byte_arr->read(get_begin(ind) + SLBottomLevelRecord::NEXT_BEGIN,
                     get_begin(ind) + SLBottomLevelRecord::NEXT_BEGIN +
                         sizeof(SLBottomLevelRecord::next));
  std::uint64_t next = 0;
  std::memcpy(&next, next_chars.data(), sizeof(next));
  return next;
}

void SLBottomLevelRecordViewer::set_next(std::uint64_t ind,
                                         std::uint64_t new_next) {
  std::vector<ByteType> next_chars(sizeof(new_next));
  std::memcpy(next_chars.data(), &new_next, sizeof(new_next));
  byte_arr->rewrite(get_begin(ind) + SLBottomLevelRecord::NEXT_BEGIN,
                    next_chars);
}

void SLBottomLevelRecordViewer::set_offset(std::uint64_t ind,
                                           std::uint64_t new_offset) {
  std::vector<ByteType> next_chars(sizeof(new_offset));
  std::memcpy(next_chars.data(), &new_offset, sizeof(new_offset));
  byte_arr->rewrite(get_begin(ind) + SLBottomLevelRecord::OFFSET_BEGIN,
                    next_chars);
}

SLBottomLevelRecord SLBottomLevelRecordViewer::operator[](std::uint64_t ind) {
  return get_record(ind);
}

std::uint64_t
SLBottomLevelRecordViewer::append_record(const SLBottomLevelRecord &record) {
  std::vector<ByteType> record_chars(SLBottomLevelRecord::SIZE);
  std::memcpy(record_chars.data() + SLBottomLevelRecord::NEXT_BEGIN,
              &record.next, sizeof(record.next));
  std::memcpy(record_chars.data() + SLBottomLevelRecord::OFFSET_BEGIN,
              &record.offset, sizeof(record.offset));
  std::memcpy(record_chars.data() + SLBottomLevelRecord::KEY_BEGIN,
              record.key.data(), KEY_SIZE_BYTES);
  byte_arr->append(record_chars);
  return byte_arr->size() / SLBottomLevelRecord::SIZE - 1;
}

std::uint64_t SLBottomLevelRecordViewer::get_elems_count() const {
  return (byte_arr->size() - HEAD_SIZE) / SLBottomLevelRecord::SIZE;
}

SLUpperLevelRecord::SLUpperLevelRecord(const KeyType &key_, std::uint64_t down_)
    : down(down_), key(key_) {}

bool operator==(const SLUpperLevelRecord &record1,
                const SLUpperLevelRecord &record2) {
  return record1.next == record2.next && record1.down == record2.down &&
         record1.key == record2.key;
}

SLUpperLevelRecordViewer::SLUpperLevelRecordViewer(ByteArrayPtr byte_arr_,
                                                   ByteArrayPtr heads_)
    : byte_arr(byte_arr_), heads(heads_) {}

std::uint64_t SLUpperLevelRecordViewer::get_levels() {
  return heads->size() / HEAD_SIZE;
}

std::uint64_t SLUpperLevelRecordViewer::get_begin(std::uint64_t ind) {
  return ind * SLUpperLevelRecord::SIZE;
}

std::uint64_t SLUpperLevelRecordViewer::get_head(std::uint64_t list_ind) {
  std::vector<ByteType> head_chars =
      heads->read(list_ind * HEAD_SIZE, (list_ind + 1) * HEAD_SIZE);
  std::uint64_t head = 0;
  std::memcpy(&head, head_chars.data(), HEAD_SIZE);
  return head;
}

void SLUpperLevelRecordViewer::set_head(std::uint64_t list_ind,
                                        std::uint64_t head) {
  std::vector<ByteType> head_chars(HEAD_SIZE);
  std::memcpy(head_chars.data(), &head, HEAD_SIZE);
  heads->rewrite(list_ind * HEAD_SIZE, head_chars);
}

void SLUpperLevelRecordViewer::append_head(std::uint64_t head) {
  std::vector<ByteType> head_chars(HEAD_SIZE);
  std::memcpy(head_chars.data(), &head, HEAD_SIZE);
  heads->append(head_chars);
}

SLUpperLevelRecord SLUpperLevelRecordViewer::get_record(std::uint64_t ind) {
  std::vector<ByteType> record_chars =
      byte_arr->read(get_begin(ind), get_begin(ind + 1));
  SLUpperLevelRecord record;
  std::memcpy(&record.next,
              record_chars.data() + SLUpperLevelRecord::NEXT_BEGIN,
              sizeof(record.next));
  std::memcpy(&record.down,
              record_chars.data() + SLUpperLevelRecord::DOWN_BEGIN,
              sizeof(record.down));
  std::memcpy(record.key.data(),
              record_chars.data() + SLUpperLevelRecord::KEY_BEGIN,
              KEY_SIZE_BYTES);
  return record;
}

std::uint64_t SLUpperLevelRecordViewer::get_next(std::uint64_t ind) {
  std::vector<ByteType> next_chars =
      byte_arr->read(get_begin(ind) + SLUpperLevelRecord::NEXT_BEGIN,
                     get_begin(ind) + SLUpperLevelRecord::NEXT_BEGIN +
                         sizeof(SLUpperLevelRecord::next));
  std::uint64_t next = 0;
  std::memcpy(&next, next_chars.data(), sizeof(next));
  return next;
}

void SLUpperLevelRecordViewer::set_next(std::uint64_t ind,
                                        std::uint64_t new_next) {
  std::vector<ByteType> next_chars(sizeof(new_next));
  std::memcpy(next_chars.data(), &new_next, sizeof(new_next));
  byte_arr->rewrite(get_begin(ind) + SLUpperLevelRecord::NEXT_BEGIN,
                    next_chars);
}

SLUpperLevelRecord SLUpperLevelRecordViewer::operator[](std::uint64_t ind) {
  return get_record(ind);
}

std::uint64_t
SLUpperLevelRecordViewer::append_record(const SLUpperLevelRecord &record) {
  std::vector<ByteType> record_chars(SLUpperLevelRecord::SIZE);
  std::memcpy(record_chars.data() + SLUpperLevelRecord::NEXT_BEGIN,
              &record.next, sizeof(record.next));
  std::memcpy(record_chars.data() + SLUpperLevelRecord::DOWN_BEGIN,
              &record.down, sizeof(record.down));
  std::memcpy(record_chars.data() + SLUpperLevelRecord::KEY_BEGIN,
              record.key.data(), KEY_SIZE_BYTES);
  byte_arr->append(record_chars);
  return byte_arr->size() / SLUpperLevelRecord::SIZE - 1;
}

} // namespace kvaaas
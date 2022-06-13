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
  std::uint64_t head = 0;
  byte_arr->read_ptr(reinterpret_cast<ByteType *>(&head), 0, HEAD_SIZE);
  return head;
}

void SLBottomLevelRecordViewer::set_head(std::uint64_t head) {
  byte_arr->rewrite(0, reinterpret_cast<ByteType *>(&head), HEAD_SIZE);
}

SLBottomLevelRecord SLBottomLevelRecordViewer::get_record(std::uint64_t ind) {
  SLBottomLevelRecord record;
    byte_arr->read_ptr(reinterpret_cast<ByteType *>(&record.next),
                        get_begin(ind) + SLBottomLevelRecord::NEXT_BEGIN,
                        get_begin(ind) + SLBottomLevelRecord::NEXT_BEGIN + sizeof(record.next));

    byte_arr->read_ptr(reinterpret_cast<ByteType *>(&record.offset),
                       get_begin(ind) + SLBottomLevelRecord::OFFSET_BEGIN,
                       get_begin(ind) + SLBottomLevelRecord::OFFSET_BEGIN + sizeof(record.offset));

    byte_arr->read_ptr(reinterpret_cast<ByteType *>(record.key.data()),
                       get_begin(ind) + SLBottomLevelRecord::KEY_BEGIN,
                       get_begin(ind) + SLBottomLevelRecord::KEY_BEGIN + record.key.size());
  return record;
}

std::uint64_t SLBottomLevelRecordViewer::get_next(std::uint64_t ind) {
  std::uint64_t next = 0;
      byte_arr->read_ptr( reinterpret_cast<ByteType *>(&next),
                     get_begin(ind) + SLBottomLevelRecord::NEXT_BEGIN,
                     get_begin(ind) + SLBottomLevelRecord::NEXT_BEGIN +
                         sizeof(SLBottomLevelRecord::next));
  return next;
}

void SLBottomLevelRecordViewer::set_next(std::uint64_t ind,
                                         std::uint64_t new_next) {
  byte_arr->rewrite(get_begin(ind) + SLBottomLevelRecord::NEXT_BEGIN,
                    reinterpret_cast<ByteType *>(&new_next),
                    sizeof(new_next));
}

void SLBottomLevelRecordViewer::set_offset(std::uint64_t ind,
                                           std::uint64_t new_offset) {
    byte_arr->rewrite(get_begin(ind) + SLBottomLevelRecord::OFFSET_BEGIN,
                      reinterpret_cast<ByteType *>(&new_offset),
                      sizeof(new_offset));
}

SLBottomLevelRecord SLBottomLevelRecordViewer::operator[](std::uint64_t ind) {
  return get_record(ind);
}

std::uint64_t
SLBottomLevelRecordViewer::append_record(const SLBottomLevelRecord &record) {
    byte_arr->append(reinterpret_cast<const ByteType *>(&record.next), sizeof(record.next));
    byte_arr->append(reinterpret_cast<const ByteType *>(&record.offset), sizeof(record.offset));
    byte_arr->append(reinterpret_cast<const ByteType *>(record.key.data()), KEY_SIZE_BYTES);
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
  std::uint64_t head = 0;
  heads->read_ptr(reinterpret_cast<ByteType *>(&head), list_ind * HEAD_SIZE, (list_ind + 1) * HEAD_SIZE);
  return head;
}

void SLUpperLevelRecordViewer::set_head(std::uint64_t list_ind,
                                        std::uint64_t head) {
  heads->rewrite(list_ind * HEAD_SIZE, reinterpret_cast<const ByteType *>(&head), HEAD_SIZE);
}

void SLUpperLevelRecordViewer::append_head(std::uint64_t head) {
  heads->append(reinterpret_cast<const ByteType *>(&head), HEAD_SIZE);
}

SLUpperLevelRecord SLUpperLevelRecordViewer::get_record(std::uint64_t ind) {
    SLUpperLevelRecord record;
    byte_arr->read_ptr(reinterpret_cast<ByteType *>(&record.next),
                       get_begin(ind) + SLUpperLevelRecord::NEXT_BEGIN,
                       get_begin(ind) + SLUpperLevelRecord::NEXT_BEGIN + sizeof(record.next));

    byte_arr->read_ptr(reinterpret_cast<ByteType *>(&record.down),
                       get_begin(ind) + SLUpperLevelRecord::DOWN_BEGIN,
                       get_begin(ind) + SLUpperLevelRecord::DOWN_BEGIN + sizeof(record.down));

    byte_arr->read_ptr(reinterpret_cast<ByteType *>(record.key.data()),
                       get_begin(ind) + SLUpperLevelRecord::KEY_BEGIN,
                       get_begin(ind) + SLUpperLevelRecord::KEY_BEGIN + record.key.size());
  return record;
}

std::uint64_t SLUpperLevelRecordViewer::get_next(std::uint64_t ind) {
  std::uint64_t next = 0;
  byte_arr->read_ptr(reinterpret_cast<ByteType *>(&next), get_begin(ind), get_begin(ind) + sizeof(SLUpperLevelRecord::next));
  return next;
}

void SLUpperLevelRecordViewer::set_next(std::uint64_t ind,
                                        std::uint64_t new_next) {
  byte_arr->rewrite(get_begin(ind) + SLUpperLevelRecord::NEXT_BEGIN,
                    reinterpret_cast<const ByteType *>(&new_next),
                    sizeof(new_next));
}

SLUpperLevelRecord SLUpperLevelRecordViewer::operator[](std::uint64_t ind) {
  return get_record(ind);
}

std::uint64_t
SLUpperLevelRecordViewer::append_record(const SLUpperLevelRecord &record) {
  byte_arr->append(reinterpret_cast<const ByteType *>(&record.next), sizeof(record.next));
  byte_arr->append(reinterpret_cast<const ByteType *>(&record.down), sizeof(record.down));
  byte_arr->append(reinterpret_cast<const ByteType *>(record.key.data()), KEY_SIZE_BYTES);
  return byte_arr->size() / SLUpperLevelRecord::SIZE - 1;
}

} // namespace kvaaas
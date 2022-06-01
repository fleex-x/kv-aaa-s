#include "SkipListRecords.h"
#include "ByteArray.h"
#include <vector>
#include <cstring>
#include "Core.h"

namespace kvaaas {

bool operator==(const SLBottomLevelRecord &record1, const SLBottomLevelRecord &record2) {
    return record1.next == record2.next && record1.offset == record2.offset &&
           record1.key == record2.key;
           //std::memcmp(record1.key.data(), record2.key.data(), SLBottomLevelRecord::KEY_SIZE) == 0;
}

SLBottomLevelRecordViewer::SLBottomLevelRecordViewer(ByteArrayPtr byte_arr_) :
    byte_arr(byte_arr_) {
    byte_arr->append(std::vector<ByteType>(HEAD_SIZE));
}

std::uint64_t SLBottomLevelRecordViewer::get_begin(std::uint64_t ind) {
    return HEAD_SIZE + ind * SLBottomLevelRecord::SIZE;
}

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
    std::vector<ByteType> record_chars = byte_arr->read(get_begin(ind), get_begin(ind + 1));
    SLBottomLevelRecord record;
    std::memcpy(&record.next, record_chars.data() + SLBottomLevelRecord::NEXT_BEGIN, sizeof(record.next));
    std::memcpy(&record.offset, record_chars.data() + SLBottomLevelRecord::OFFSET_BEGIN, sizeof(record.offset));
    std::memcpy(record.key.data(), record_chars.data() + SLBottomLevelRecord::KEY_BEGIN, KEY_SIZE_BYTES);
    return record;
}

SLBottomLevelRecord SLBottomLevelRecordViewer::operator[](std::uint64_t ind) {
    return get_record(ind);
}


void SLBottomLevelRecordViewer::append_record(const SLBottomLevelRecord &record) {
    std::vector<ByteType> record_chars(SLBottomLevelRecord::SIZE);
    std::memcpy(record_chars.data() + SLBottomLevelRecord::NEXT_BEGIN,&record.next,  sizeof(record.next));
    std::memcpy(record_chars.data() + SLBottomLevelRecord::OFFSET_BEGIN, &record.offset, sizeof(record.offset));
    std::memcpy(record_chars.data() + SLBottomLevelRecord::KEY_BEGIN, record.key.data(), KEY_SIZE_BYTES);
    byte_arr->append(record_chars);
}






bool operator==(const SLUpperLevelRecord &record1, const SLUpperLevelRecord &record2) {
    return record1.next == record2.next && record1.offset == record2.offset &&
           record1.down == record2.down && record1.key == record2.key;
}

SLUpperLevelRecordViewer::SLUpperLevelRecordViewer(ByteArrayPtr byte_arr_, ByteArrayPtr heads_) :
        byte_arr(byte_arr_),
        heads(heads_) {
}

std::uint64_t SLUpperLevelRecordViewer::get_begin(std::uint64_t ind) {
    return ind * SLUpperLevelRecord::SIZE;
}

std::uint64_t SLUpperLevelRecordViewer::get_head(std::uint64_t list_ind) {
    std::vector<ByteType> head_chars = heads->read(list_ind * HEAD_SIZE, (list_ind + 1) * HEAD_SIZE);
    std::uint64_t head = 0;
    std::memcpy(&head, head_chars.data(), HEAD_SIZE);
    return head;
}

void SLUpperLevelRecordViewer::set_head(std::uint64_t list_ind, std::uint64_t head) {
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
    std::vector<ByteType> record_chars = byte_arr->read(get_begin(ind), get_begin(ind + 1));
    SLUpperLevelRecord record;
    std::memcpy(&record.next, record_chars.data() + SLUpperLevelRecord::NEXT_BEGIN, sizeof(record.next));
    std::memcpy(&record.down, record_chars.data() + SLUpperLevelRecord::DOWN_BEGIN, sizeof(record.down));
    std::memcpy(&record.offset, record_chars.data() + SLUpperLevelRecord::OFFSET_BEGIN, sizeof(record.offset));
    std::memcpy(record.key.data(), record_chars.data() + SLUpperLevelRecord::KEY_BEGIN, KEY_SIZE_BYTES);
    return record;
}

SLUpperLevelRecord SLUpperLevelRecordViewer::operator[](std::uint64_t ind) {
    return get_record(ind);
}

void SLUpperLevelRecordViewer::append_record(const SLUpperLevelRecord &record) {
    std::vector<ByteType> record_chars(SLUpperLevelRecord::SIZE);
    std::memcpy(record_chars.data() + SLUpperLevelRecord::NEXT_BEGIN,&record.next,  sizeof(record.next));
    std::memcpy(record_chars.data() + SLUpperLevelRecord::DOWN_BEGIN,&record.down,  sizeof(record.down));
    std::memcpy(record_chars.data() + SLUpperLevelRecord::OFFSET_BEGIN, &record.offset, sizeof(record.offset));
    std::memcpy(record_chars.data() + SLUpperLevelRecord::KEY_BEGIN, record.key.data(), KEY_SIZE_BYTES);
    byte_arr->append(record_chars);
}

}
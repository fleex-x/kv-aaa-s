#include "SkipListRecords.h"
#include "ByteArray.h"
#include <vector>
#include <cstring>

namespace kvaaas {

bool cmp_key(const unsigned char *key1, const unsigned char *key2) {
    for (std::size_t i = 0; i < SLBottomLevelRecord::KEY_SIZE; ++i) {
        if (key1[i] != key2[i]) {
            return key1[i] < key2[i];
        }
    }
    return false;
}

bool operator==(const SLBottomLevelRecord &record1, const SLBottomLevelRecord &record2) {
    return record1.next == record2.next && record1.offset == record2.offset &&
           std::memcmp(record1.key, record2.key, SLBottomLevelRecord::KEY_SIZE) == 0;
}

SLBottomLevelRecordViewer::SLBottomLevelRecordViewer(ByteArrayPtr byte_arr_) :
    byte_arr(byte_arr_) {
    byte_arr->append(std::vector<unsigned char>(HEAD_SIZE, 0));
}

std::size_t SLBottomLevelRecordViewer::get_begin(std::size_t ind) {
    return HEAD_SIZE + ind * SLBottomLevelRecord::SIZE;
}

std::size_t SLBottomLevelRecordViewer::get_head() {
    std::vector<unsigned char> head_chars = byte_arr->read(0, HEAD_SIZE);
    std::size_t head = 0;
    std::memcpy(&head, head_chars.data(), HEAD_SIZE);
    return head;
}

void SLBottomLevelRecordViewer::set_head(std::size_t head) {
    std::vector<unsigned char> head_chars(HEAD_SIZE, 0);
    std::memcpy(head_chars.data(), &head, HEAD_SIZE);
    byte_arr->rewrite(0, head_chars);
}

SLBottomLevelRecord SLBottomLevelRecordViewer::get_record(std::size_t ind) {
    std::vector<unsigned char> record_chars = byte_arr->read(get_begin(ind), get_begin(ind + 1));
    SLBottomLevelRecord record;
    std::memcpy(&record.next, record_chars.data() + SLBottomLevelRecord::NEXT_BEGIN, sizeof(record.next));
    std::memcpy(&record.offset, record_chars.data() + SLBottomLevelRecord::OFFSET_BEGIN, sizeof(record.offset));
    std::memcpy(record.key, record_chars.data() + SLBottomLevelRecord::KEY_BEGIN, sizeof(record.key));
    return record;
}

void SLBottomLevelRecordViewer::append_record(const SLBottomLevelRecord &record) {
    std::vector<unsigned char> record_chars(SLBottomLevelRecord::SIZE, 0);
    std::memcpy(record_chars.data() + SLBottomLevelRecord::NEXT_BEGIN,&record.next,  sizeof(record.next));
    std::memcpy(record_chars.data() + SLBottomLevelRecord::OFFSET_BEGIN, &record.offset, sizeof(record.offset));
    std::memcpy(record_chars.data() + SLBottomLevelRecord::KEY_BEGIN, record.key, sizeof(record.key));
    byte_arr->append(record_chars);
}

}
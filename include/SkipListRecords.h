#pragma once
#include "ByteArray.h"

namespace kvaaas {

struct SLBottomLevelRecord {
    static constexpr std::size_t KEY_SIZE = 4;
    std::size_t next = 0;
    std::size_t offset = 0;
    unsigned char key[KEY_SIZE]{};
    static constexpr std::size_t SIZE = sizeof(next) + sizeof(offset) + sizeof(key);
    static constexpr std::size_t NEXT_BEGIN = 0;
    static constexpr std::size_t OFFSET_BEGIN = sizeof(next);
    static constexpr std::size_t KEY_BEGIN = sizeof(next) + sizeof(offset);
};

bool operator==(const SLBottomLevelRecord &record1,const SLBottomLevelRecord &record2);

bool cmp_key(const unsigned char *key1, const unsigned char *key2);

class SLBottomLevelRecordViewer {
private:
    ByteArrayPtr byte_arr;
    static constexpr std::size_t HEAD_SIZE = sizeof(SLBottomLevelRecord::next);
    std::size_t get_begin(std::size_t ind);
public:
    explicit SLBottomLevelRecordViewer(ByteArrayPtr byte_arr_);
    SLBottomLevelRecord get_record(std::size_t ind);
    std::size_t get_head();
    void append_record(const SLBottomLevelRecord &record);
    void set_head(std::size_t head);
};

}
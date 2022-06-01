#pragma once
#include "ByteArray.h"
#include "Core.h"
#include <cstdint>

namespace kvaaas {

struct SLBottomLevelRecord {
    static constexpr std::uint64_t KEY_SIZE = KEY_SIZE_BYTES;
    std::uint64_t next = 0;
    std::uint64_t offset = 0;
    KeyType key{};
    static constexpr std::uint64_t SIZE = sizeof(next) + sizeof(offset) + sizeof(key);
    static constexpr std::uint64_t NEXT_BEGIN = 0;
    static constexpr std::uint64_t OFFSET_BEGIN = sizeof(next);
    static constexpr std::uint64_t KEY_BEGIN = sizeof(next) + sizeof(offset);
};

bool operator==(const SLBottomLevelRecord &record1,const SLBottomLevelRecord &record2);

class SLBottomLevelRecordViewer {
private:
    ByteArrayPtr byte_arr;
    static constexpr std::uint64_t HEAD_SIZE = sizeof(SLBottomLevelRecord::next);
    static std::uint64_t get_begin(std::uint64_t ind);
public:
    explicit SLBottomLevelRecordViewer(ByteArrayPtr byte_arr_);
    SLBottomLevelRecord get_record(std::uint64_t ind);
    std::uint64_t get_head();
    void append_record(const SLBottomLevelRecord &record);
    void set_head(std::uint64_t head);
};

struct SLUpperLevelRecord {
    static constexpr std::uint64_t KEY_SIZE = KEY_SIZE_BYTES;
    std::uint64_t next = 0;
    std::uint64_t down = 0;
    std::uint64_t offset = 0;
    KeyType key{};
    static constexpr std::uint64_t SIZE = sizeof(next) +  sizeof(down) + sizeof(offset) + sizeof(key);
    static constexpr std::uint64_t NEXT_BEGIN = 0;
    static constexpr std::uint64_t DOWN_BEGIN = sizeof(down);
    static constexpr std::uint64_t OFFSET_BEGIN = sizeof(down) + sizeof(next);
    static constexpr std::uint64_t KEY_BEGIN = sizeof(down) + sizeof(next) + sizeof(offset);
};

bool operator==(const SLUpperLevelRecord &record1,const SLUpperLevelRecord &record2);

class SLUpperLevelRecordViewer {
private:
    ByteArrayPtr byte_arr;
    ByteArrayPtr heads;
    static constexpr std::uint64_t HEAD_SIZE = sizeof(SLUpperLevelRecord::next);
    static std::uint64_t get_begin(std::uint64_t ind);
public:
    explicit SLUpperLevelRecordViewer(ByteArrayPtr byte_arr_, ByteArrayPtr heads_);
    SLUpperLevelRecord get_record(std::uint64_t ind);
    void append_record(const SLUpperLevelRecord &record);
    std::uint64_t get_head(std::uint64_t list_ind);
    void set_head(std::uint64_t list_ind, std::uint64_t head);
    void append_head(std::uint64_t head);
};

}
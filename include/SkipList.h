#pragma once
#include "Core.h"
#include "SkipListRecords.h"

namespace kvaaas {

class SkipList {
private:
    SLBottomLevelRecordViewer bottom;
    SLUpperLevelRecordViewer upper;
    std::size_t levels_count;
public:
    SkipList(const SLBottomLevelRecordViewer &bottom_, const SLUpperLevelRecordViewer &upper_);
    void add(KeyType key, std::uint64_t offset);
};

}
#pragma once
#include <vector>
#include <optional>
#include <cassert>
#include "ByteArray.h"

enum class FilePurpose {
    KVS = 0,
    SST = 1,
    SKIP_LIST_UL = 2,
    SKIP_LIST_BL = 3
};

class FileType {
private:
    FilePurpose fp;
    std::optional<std::size_t> sst_level;
public:
    explicit FileType(FilePurpose fp_, std::optional<std::size_t> sst_level_ = {});

    [[nodiscard]] FilePurpose get_file_purpose() const;

    [[nodiscard]] std::size_t get_sst_level() const;

};

class FileManager {
public:
    virtual ByteArray* get_file(FileType ft) = 0;
    virtual ByteArray* create_file(FileType ft) = 0;
    virtual ByteArray* start_overwrite(FileType ft) = 0;
    virtual void end_overwrite(FileType ft) = 0;
    virtual void remove(FileType ft) = 0;

    virtual ~FileManager() = default;
};


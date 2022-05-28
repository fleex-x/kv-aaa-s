#include "FileManager.h"
#include <vector>
#include <optional>
#include <cassert>

FileType::FileType(FilePurpose fp_, std::optional<std::size_t> sst_level_) :
    fp(fp_),
    sst_level(sst_level_) {
    if (fp_ == FilePurpose::SST) {
        assert(sst_level.has_value());
    }
}

FilePurpose FileType::get_file_purpose() const {
    return fp;
}

std::size_t FileType::get_sst_level() const {
    return sst_level.value();
}
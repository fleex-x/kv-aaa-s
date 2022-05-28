#pragma once
#include <string>

namespace kvaaas {

enum class ErrorStatus {
    DISK_READING_ERROR = 0
};

class Error {
private:
    ErrorStatus status;
public:
    explicit Error(ErrorStatus status_);

    std::string to_string();

    ErrorStatus get_status();
};

}
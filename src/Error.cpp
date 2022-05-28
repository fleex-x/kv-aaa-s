#include "Error.h"
#include <cassert>

namespace kvaaas {

Error::Error(ErrorStatus status_) : status(status_) {};

std::string Error::to_string() {
    switch (status) {
        case ErrorStatus::DISK_READING_ERROR:
            return "Disk reading error";
    }
    assert(false);
};

ErrorStatus Error::get_status() {
    return status;
}

}

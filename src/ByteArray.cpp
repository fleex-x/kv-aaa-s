#include "ByteArray.h"

namespace kvaaas {

void RAMByteArray::append(const std::vector<unsigned char> &bytes)  {
    byte_array.insert(byte_array.end(), bytes.begin(), bytes.end());
}

void RAMByteArray::rewrite(std::size_t begin, const std::vector<unsigned char> &bytes) {
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        byte_array[begin + i] = bytes[i];
    }
}

std::vector<unsigned char> RAMByteArray::read(std::size_t l, std::size_t r)  {
    return {byte_array.begin() + l, byte_array.begin() + r};
}

std::size_t RAMByteArray::size()  {
    return byte_array.size();
}

}
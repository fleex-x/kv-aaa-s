#include "ByteArray.h"

namespace kvaaas {

void RAMByteArray::append(const std::vector<unsigned char> &bytes)  {
    byte_array.insert(byte_array.end(), bytes.begin(), bytes.end());
}

std::vector<unsigned char> RAMByteArray::read(int l, int r)  {
    return {byte_array.begin() + l, byte_array.begin() + r};
}

std::size_t RAMByteArray::size()  {
    return byte_array.size();
}

}
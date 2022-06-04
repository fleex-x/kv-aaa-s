#include "ByteArray.h"
#include "Core.h"

namespace kvaaas {

void RAMByteArray::append(const std::vector<ByteType> &bytes) {
  byte_array.insert(byte_array.end(), bytes.begin(), bytes.end());
}

void RAMByteArray::rewrite(std::size_t begin,
                           const std::vector<ByteType> &bytes) {
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    byte_array[begin + i] = bytes[i];
  }
}

std::vector<ByteType> RAMByteArray::read(std::size_t l, std::size_t r) {
  return {byte_array.begin() + l, byte_array.begin() + r};
}

std::size_t RAMByteArray::size() { return byte_array.size(); }

FileByteArray::FileByteArray(const std::string &s)
    : data(s, std::ios::binary | std::ios::in | std::ios::out) {}

void FileByteArray::append(const std::vector<ByteType> &bytes) {
  data.write(
      reinterpret_cast<const char *>(bytes.data()),
      bytes.size()); /// TODO Где-то здесь потенциально много ошибок вылетает
}

void FileByteArray::rewrite(std::size_t begin,
                            const std::vector<ByteType> &bytes) {
  auto pos = data.tellp();
  data.seekp(begin); /// И здесь тоже
  data.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
  data.seekp(pos);
}
std::vector<ByteType> FileByteArray::read(std::size_t l, std::size_t r) {
  auto pos = data.tellp();
  data.seekp(l); /// -_-
  std::vector<ByteType> byte_array;
  data.get(reinterpret_cast<char *>(byte_array.data()), r - l);
  data.seekp(pos);
}

std::size_t FileByteArray::size() { return data.tellg(); }

FileByteArray::~FileByteArray() { data.close(); }

} // namespace kvaaas
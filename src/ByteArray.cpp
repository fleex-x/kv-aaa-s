#include "ByteArray.h"
#include "Core.h"
#include <filesystem>

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

FileByteArray::FileByteArray(const std::string &s) : file_name(s) {
  if (std::filesystem::exists(s) && !BA_TESTMODE) {
    data.open(s, std::fstream::ate | std::fstream::binary | std::fstream ::in |
                     std::fstream::out);
  } else {
    data.open(s, std::fstream::trunc | std::fstream::binary |
                     std::fstream ::in | std::fstream::out);
  }
}

void FileByteArray::append(const std::vector<ByteType> &bytes) {
  data.write(
      reinterpret_cast<const char *>(bytes.data()),
      bytes.size()); /// TODO Где-то здесь потенциально много ошибок вылетает
}

void FileByteArray::rewrite(std::size_t begin,
                            const std::vector<ByteType> &bytes) {
  data.seekp(begin); /// И здесь тоже
  data.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
  data.seekp(0, std::fstream::end);
}
std::vector<ByteType> FileByteArray::read(std::size_t l, std::size_t r) {
  data.seekp(l); /// -_-
  std::vector<ByteType> byte_array(r - l);
  data.read(reinterpret_cast<char *>(byte_array.data()), r - l);
  data.seekp(0, std::fstream::end);
  return byte_array;
}

std::size_t FileByteArray::size() { return data.tellp(); }

FileByteArray::~FileByteArray() {
  data.close();
  if (BA_TESTMODE) {
    std::remove(file_name.c_str());
  }
}

} // namespace kvaaas
#include "ByteArray.h"
#include "Core.h"
#include <cstring>
#include <filesystem>
#include <iostream>

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

void RAMByteArray::append(const ByteType *bytes, std::size_t n) {
  byte_array.insert(byte_array.end(), bytes, bytes + n);
}

ByteType *RAMByteArray::read_ptr(ByteType *ptr, std::size_t l, std::size_t r) {
  std::memcpy(ptr, byte_array.data() + l, r - l);
  return ptr;
}

void RAMByteArray::rewrite(std::size_t begin, const ByteType *bytes,
                           std::size_t n) {
  for (std::size_t i = 0; i < n; ++i) {
    byte_array[begin + i] = bytes[i];
  }
}

std::size_t RAMByteArray::size() { return byte_array.size(); }

FileByteArray::FileByteArray(const std::string &s, bool withRAII)
    : underlying_file(s), RAII(withRAII) {
  if (std::filesystem::exists(s) && !RAII) {
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

std::vector<ByteType> FileByteArray::read(std::size_t l, std::size_t r) {
  data.seekp(l); /// -_-
  std::vector<ByteType> byte_array(r - l);
  data.read(reinterpret_cast<char *>(byte_array.data()), r - l);
  data.seekp(0, std::fstream::end);
  return byte_array;
}

void FileByteArray::rewrite(std::size_t begin,
                            const std::vector<ByteType> &bytes) {
  data.seekp(begin);
  data.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
  data.seekp(0, std::fstream::end);
}

void FileByteArray::append(const ByteType *bytes, std::size_t n) {
  data.write(reinterpret_cast<const char *>(bytes), n);
}

ByteType *FileByteArray::read_ptr(ByteType *ptr, std::size_t l, std::size_t r) {
  data.seekp(l);
  data.read(reinterpret_cast<char *>(ptr), r - l);
  data.seekp(0, std::fstream::end);
  return ptr;
}

void FileByteArray::rewrite(std::size_t begin, const ByteType *bytes,
                            std::size_t n) {
  data.seekp(begin);
  data.write(reinterpret_cast<const char *>(bytes), n);
  data.seekp(0, std::fstream::end);
}

std::size_t FileByteArray::size() { return data.tellp(); }

FileByteArray::~FileByteArray() {
  data.close();
  if (RAII) {
    std::remove(underlying_file.c_str());
  }
}

} // namespace kvaaas

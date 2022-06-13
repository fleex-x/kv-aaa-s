//
// Created by vladimir on 04.06.22.
//

#include "doctest.h"
#include <KVSRecordsViewer.h>
#include <iostream>
#include <random>

using namespace kvaaas;

namespace {
std::vector<ByteType> gen_random(int n) {

  std::vector<ByteType> arr(n);
  std::random_device rd;
  std::mt19937_64 rnd(rd());
  std::uniform_int_distribution<unsigned char> dist;

  for (auto &b : arr) {
    b = static_cast<std::byte>(dist(rnd));
  }

  return arr;
}

ByteType *gen_random_ptr(int n) {

  auto *ptr = new ByteType[n];
  std::random_device rd;
  std::mt19937_64 rnd(rd());
  std::uniform_int_distribution<unsigned char> dist;

  for (int i = 0; i < n; i++) {
    ptr[i] = static_cast<std::byte>(dist(rnd));
  }

  return ptr;
}

} // namespace

// TEST_CASE("Create") { FileByteArray arr("fileArray"); }
//
// TEST_CASE("Append") {
//   FileByteArray arr("fileArray", true);
//   auto testArr = gen_random(1000);
//   arr.append(testArr);
//
//   CHECK(arr.read(0, 1000) == testArr);
// }
//
// TEST_CASE("Multiple append") {
//   FileByteArray arr("fileArray", true);
//   std::vector<std::vector<ByteType>> bArrs(100);
//
//   for (auto &bArr : bArrs) {
//     bArr = gen_random(1000);
//     arr.append(bArr);
//   }
//
//   std::uint64_t offset = 0;
//   for (auto &bArr : bArrs) {
//     CHECK(arr.read(offset, offset + 1000) == bArr);
//     offset += 1000;
//   }
// }
//
// TEST_CASE("Multiple append, but different offsets") {
//   FileByteArray arr("fileArray", true);
//   std::vector<std::vector<ByteType>> bArrs(100);
//
//   std::random_device rd;
//   std::mt19937_64 rnd(rd());
//   std::uniform_int_distribution<int> dist(10000, 50000);
//
//   for (auto &bArr : bArrs) {
//     bArr = gen_random(dist(rnd));
//     arr.append(bArr);
//   }
//
//   std::uint64_t offset = 0;
//   for (auto &bArr : bArrs) {
//     CHECK(arr.read(offset, offset + bArr.size()) == bArr);
//     offset += bArr.size();
//   }
// }
//
// TEST_CASE("Rewrite") {
//   FileByteArray arr("fileArray", true);
//   auto startArr = gen_random(200000);
//   arr.append(startArr);
//
//   CHECK(startArr == arr.read(0, startArr.size()));
//
//   std::random_device rd;
//   std::mt19937_64 rnd(rd());
//   std::uniform_int_distribution<int> dist(10000, 50000);
//
//   int l = dist(rnd);
//   int r = l + dist(rnd);
//
//   auto changedArr = gen_random(r - l);
//
//   for (int i = 0, j = l; j < r; j++, i++) {
//     startArr[j] = changedArr[i];
//   }
//
//   CHECK(arr.read(0, startArr.size()) != startArr);
//
//   arr.rewrite(l, changedArr);
//   CHECK(arr.size() == startArr.size());
//
//   CHECK(startArr == arr.read(0, startArr.size()));
//
//   CHECK(arr.size() == startArr.size());
// }
//
// TEST_CASE("Size") {
//   FileByteArray arr("fileArray", true);
//   auto testArr = gen_random(10000);
//   arr.append(testArr);
//   CHECK(arr.size() == 10000);
//
//   arr.append(testArr);
//   CHECK(arr.size() == 20000);
// }

TEST_CASE("Append, but char*") {
  FileByteArray arr("fileArray", true);
  auto testArr = gen_random_ptr(1000);
  arr.append(testArr, 1000);

  ByteType *ptr = new ByteType[1000];
  arr.read_ptr(ptr, 0, 1000);
  for (int j = 0; j < 1000; j++) {
    CHECK(ptr[j] == testArr[j]);
  }
  delete[] ptr;
  delete[] testArr;
}

TEST_CASE("Multiple append, but char*") {
  FileByteArray arr("fileArray", true);
  std::vector<ByteType *> bArrs(100);

  for (auto &bArr : bArrs) {
    bArr = gen_random_ptr(1000);
    arr.append(bArr, 1000);
  }

  std::uint64_t offset = 0;
  auto *ptr = new ByteType[1000];
  for (auto &bArr : bArrs) {
    arr.read_ptr(ptr, offset, offset + 1000);
    for (int i = 0; i < 1000; i++) {
      CHECK(ptr[i] == bArr[i]);
    }
    offset += 1000;
  }
  delete[] ptr;
  for (auto &b_ptr : bArrs) {
    delete[] b_ptr;
  }
}

TEST_CASE("Multiple append, but different offsets and char*") {
  FileByteArray arr("fileArray", true);
  std::vector<ByteType *> bArrs(100);
  std::vector<int> sizes(100);
  std::random_device rd;
  std::mt19937_64 rnd(rd());
  std::uniform_int_distribution<int> dist(10000, 50000);

  int i = 0;
  for (auto &bArr : bArrs) {
    int n = dist(rnd);
    bArr = gen_random_ptr(n);
    arr.append(bArr, n);
    sizes[i++] = n;
  }

  auto *ptr = new ByteType[50000];

  std::uint64_t offset = 0;
  for (std::size_t i = 0; i < bArrs.size(); i++) {
    arr.read_ptr(ptr, offset, offset + sizes[i]);
    for (int j = 0; j < sizes[i]; j++) {
      CHECK(ptr[j] == bArrs[i][j]);
    }
    offset += sizes[i];
  }
  delete[] ptr;

  for (auto &b_ptr : bArrs) {
    delete[] b_ptr;
  }
}

TEST_CASE("Rewrite, but char*") {
  FileByteArray arr("fileArray", true);
  auto startArr = gen_random_ptr(200'000);
  arr.append(startArr, 200'000);
  auto *ptr = new ByteType[200'000];
  arr.read_ptr(ptr, 0, 200'000);
  for (std::size_t j = 0; j < 200'000; j++) {
    CHECK(startArr[j] == ptr[j]);
  }

  std::random_device rd;
  std::mt19937_64 rnd(rd());
  std::uniform_int_distribution<int> dist(10000, 50000);

  int l = dist(rnd);
  int r = l + dist(rnd);

  auto changedArr = gen_random_ptr(r - l);

  for (int i = 0, j = l; j < r; j++, i++) {
    startArr[j] = changedArr[i];
  }

  arr.rewrite(l, changedArr, r - l);
  CHECK(arr.size() == 200'000);

  arr.read_ptr(ptr, 0, 200'000);
  for (std::size_t j = 0; j < 200'000; j++) {
    CHECK(startArr[j] == ptr[j]);
  }

  CHECK(arr.size() == 200'000);
  delete[] changedArr;
  delete[] startArr;
  delete[] ptr;
}

TEST_CASE("Size, but char*") {
  FileByteArray arr("fileArray", true);
  auto *testArr = gen_random_ptr(10000);
  arr.append(testArr, 10000);
  CHECK(arr.size() == 10000);

  arr.append(testArr, 10000);
  CHECK(arr.size() == 20000);
  delete[] testArr;
}
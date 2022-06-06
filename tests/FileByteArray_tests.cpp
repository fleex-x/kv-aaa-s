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
} // namespace

TEST_CASE("Create") { FileByteArray arr("fileArray"); }

TEST_CASE("Append") {
  FileByteArray arr("fileArray", true);
  auto testArr = gen_random(1000);
  arr.append(testArr);

  CHECK(arr.read(0, 1000) == testArr);
}

TEST_CASE("Multiple append") {
  FileByteArray arr("fileArray", true);
  std::vector<std::vector<ByteType>> bArrs(100);

  for (auto &bArr : bArrs) {
    bArr = gen_random(1000);
    arr.append(bArr);
  }

  std::uint64_t offset = 0;
  for (auto &bArr : bArrs) {
    CHECK(arr.read(offset, offset + 1000) == bArr);
    offset += 1000;
  }
}

TEST_CASE("Multiple append, but different offsets") {
  FileByteArray arr("fileArray", true);
  std::vector<std::vector<ByteType>> bArrs(100);

  std::random_device rd;
  std::mt19937_64 rnd(rd());
  std::uniform_int_distribution<int> dist(10000, 50000);

  for (auto &bArr : bArrs) {
    bArr = gen_random(dist(rnd));
    arr.append(bArr);
  }

  std::uint64_t offset = 0;
  for (auto &bArr : bArrs) {
    CHECK(arr.read(offset, offset + bArr.size()) == bArr);
    offset += bArr.size();
  }
}

TEST_CASE("Rewrite") {
  FileByteArray arr("fileArray", true);
  auto startArr = gen_random(2000000);
  arr.append(startArr);

  CHECK(startArr == arr.read(0, startArr.size()));

  std::random_device rd;
  std::mt19937_64 rnd(rd());
  std::uniform_int_distribution<int> dist(10000, 50000);

  int l = dist(rnd);
  int r = l + dist(rnd);

  auto changedArr = gen_random(r - l);

  for (int i = 0, j = l; j < r; j++, i++) {
    startArr[j] = changedArr[i];
  }

  CHECK(arr.read(0, startArr.size()) != startArr);

  arr.rewrite(l, changedArr);
  CHECK(arr.size() == startArr.size());

  CHECK(startArr == arr.read(0, startArr.size()));

  CHECK(arr.size() == startArr.size());
}

TEST_CASE("Size") {
  FileByteArray arr("fileArray", true);
  auto testArr = gen_random(10000);
  arr.append(testArr);
  CHECK(arr.size() == 10000);

  arr.append(testArr);
  CHECK(arr.size() == 20000);
}

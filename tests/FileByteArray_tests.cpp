//
// Created by vladimir on 04.06.22.
//

#include "doctest.h"
#include <KVSRecordsViewer.h>
#include <iostream>
#include <random>

using namespace kvaaas;

namespace {
std::vector<ByteType> genRandom(int n) {

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

TEST_CASE("Create") {
  FileByteArray arr("fileArray");
}

TEST_CASE("Append") {
  FileByteArray arr("new2.txt");
  auto testArr = genRandom(1000);
  arr.append(testArr);

  CHECK(arr.read(0, 1000) == testArr);
}
#include "SST.h"
#include "Core.h"
#include "doctest.h"

#include <random>

namespace {
using namespace kvaaas;

TEST_CASE("SSTRecordsViewer") {
  FileByteArray arr("SST1");
  SSTRecordViewer viewer(&arr, NewSSTRV{});
  CHECK(viewer.size() == 0);

  SSTRecord rec;
  rec.offset = 100;
  rec.key = {std::byte(0), std::byte(0), std::byte(0)};
  viewer.append(rec);

  CHECK(viewer.size() == 1);

  CHECK(rec == viewer.get_record(0));

  rec.offset = 1234;
  rec.key = {std::byte(1), std::byte(1), std::byte(1)};
  viewer.append(rec);

  CHECK(viewer.size() == 2);
  CHECK(rec == viewer.get_record(1));
  CHECK(rec != viewer.get_record(0));
}

TEST_CASE("SSTJustWorks") {
  FileByteArray arr("SST1");
  SSTRecordViewer viewer(&arr, NewSSTRV{});

  SSTRecord rec;
  rec.offset = 100;
  rec.key = {std::byte(0), std::byte(0), std::byte(0)};
  viewer.append(rec);
  rec.offset = 1234;
  rec.key = {std::byte(1), std::byte(1), std::byte(1)};
  viewer.append(rec);

  SST sst(viewer);
  CHECK(sst.find_offset({std::byte{0}, std::byte{0}, std::byte{0}}) == 100);
  CHECK(sst.find_offset({std::byte{1}, std::byte{1}, std::byte{1}}) == 1234);
  CHECK(sst.begin() != sst.end());
}

TEST_CASE("SSTEmpty") {
  FileByteArray arr("SST1");
  SSTRecordViewer viewer(&arr, NewSSTRV{});

  SST sst(viewer);
  CHECK(sst.begin() == sst.end());
}

TEST_CASE("SSTMerge") {
  FileByteArray arr1("SST1"), arr2("SST2"), arr3("SST3");
  SSTRecordViewer view1(&arr1, NewSSTRV{}), view2(&arr2, NewSSTRV{}),
      view3(&arr3, NewSSTRV{});

  auto z = std::byte(0);
  auto t = std::byte(2);

  SSTRecord rec;

  // init first
  rec.offset = 1;
  rec.key = {z};
  view1.append(rec);

  rec.offset = 2;
  rec.key = {z, z};
  view1.append(rec);

  rec.offset = 5;
  rec.key = {t, t, t, t};
  view1.append(rec);

  // init second
  rec.offset = 3;
  rec.key = {t};
  view2.append(rec);

  rec.offset = 4;
  rec.key = {t, t};
  view2.append(rec);

  rec.offset = 100; // should not add in result SST
  rec.key = {t, t, t, t};
  view2.append(rec);

  rec.offset = 6;
  rec.key = {
      t, t, t, t, t, t, t,
  };
  view2.append(rec);

  SST sst1(view1), sst2(view2);
  SST sst3 = SST::merge_into_sst(sst1.begin(), sst1.end(), sst2.begin(),
                                 sst2.end(), view3);
  CHECK(sst3.size() == 6);

  KeyType keys[] = {{z},    {z, z},       {t},
                    {t, t}, {t, t, t, t}, {t, t, t, t, t, t, t}};
  std::uint64_t offsets[] = {1, 2, 3, 4, 5, 6};

  int i = 0;
  for (auto iter = sst3.begin(); iter != sst3.end(); ++iter, ++i) {
    const SSTRecord rec = *iter;
    CHECK(rec.key == keys[i]);
    CHECK(rec.offset == offsets[i]);
  }
}

TEST_CASE("SSTStress") {
  std::random_device rnd_device;
  std::mt19937 mersenne_engine{rnd_device()}; // Generates random integers
  std::uniform_int_distribution<unsigned> dist{
      1, static_cast<unsigned>(std::numeric_limits<std::byte>::max())};

  auto gen_byte = [&dist, &mersenne_engine]() {
    return std::byte(dist(mersenne_engine));
  };
  auto gen_key = [&] {
    KeyType key;
    for (std::size_t i = 0; i < key.size(); ++i) {
      key[i] = gen_byte();
    }
    return key;
  };
  auto gen_offset = [&] { return (std::uint64_t)dist(mersenne_engine); };

  auto gen_record = [&] { return SSTRecord{gen_key(), gen_offset()}; };

  constexpr std::size_t NUM_ELEMS = 10'000;

  std::vector<SSTRecord> vec1(NUM_ELEMS);
  std::generate(vec1.begin(), vec1.end(), gen_record);
  std::sort(vec1.begin(), vec1.end());
  vec1.erase(std::unique(vec1.begin(), vec1.end()), vec1.end());

  std::vector<SSTRecord> vec2(NUM_ELEMS);
  std::generate(vec2.begin(), vec2.end(), gen_record);
  std::sort(vec2.begin(), vec2.end());
  vec2.erase(std::unique(vec2.begin(), vec2.end()), vec2.end());

  std::vector<SSTRecord> vec3(vec1.size() + vec2.size());
  std::merge(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), vec3.begin());

  FileByteArray arr1("SST1"), arr2("SST2"), arr3("SST3");
  SSTRecordViewer view1(&arr1, NewSSTRV{}), view2(&arr2, NewSSTRV{}),
      view3(&arr3, NewSSTRV{});
  for (const auto &rec : vec1) {
    view1.append(rec);
  }
  for (const auto &rec : vec2) {
    view2.append(rec);
  }

  SST sst1(view1), sst2(view2);
  SST sst3 = SST::merge_into_sst(sst1.begin(), sst1.end(), sst2.begin(),
                                 sst2.end(), view3);
  std::vector<SSTRecord> vec_sst;
  vec_sst.reserve(NUM_ELEMS);
  for (const auto &f : sst3) {
    vec_sst.push_back(f);
  }

  CHECK(vec3 == vec_sst);
}

TEST_CASE("SSTRecordViewerRebuild") {
  FileByteArray arr("SST1");
  SSTRecordViewer view(&arr, NewSSTRV{});
  SSTRecord rec{{std::byte{0}, std::byte{0}, std::byte{0}}, 0};
  view.append(rec);
  SSTRecord rec2{{std::byte{1}, std::byte{1}, std::byte{1}}, 1};
  view.append(rec2);

  CHECK(view.size() == 2);
  
  SSTRecordViewer view2(&arr, RebuildSSTRV{});
  CHECK(view2.size() == view.size());
  CHECK(view2.get_record(0) == rec);
  CHECK(view2.get_record(1) == rec2);
}

} // namespace

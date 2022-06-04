#include "SST.h"
#include "doctest.h"

namespace {
using namespace kvaaas;

TEST_CASE("SSTRecordsViewer") {
  RAMByteArray arr;
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
  RAMByteArray arr;
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
  CHECK(!(sst.begin() == sst.end())); 
}

TEST_CASE("SSTEmpty") {
  RAMByteArray arr;
  SSTRecordViewer viewer(&arr, NewSSTRV{});

  SST sst(viewer);
  CHECK(sst.begin() == sst.end()); 
}

TEST_CASE("SSTMerge") {
  RAMByteArray arr1, arr2, arr3;
  SSTRecordViewer view1(&arr1, NewSSTRV{}), view2(&arr2, NewSSTRV{}), view3(&arr3, NewSSTRV{});
  
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

  SST sst1(view1), sst2(view2);
  SST sst3 = SST::merge_into_sst(sst1.begin(), sst1.end(), sst2.begin(), sst2.end(), view3);
  CHECK(sst3.size() == 5);
  
  KeyType keys[5] = {{z}, {z, z}, {t}, {t,t}, {t,t,t,t}};
  std::uint64_t offsets[5] = {1, 2, 3, 4, 5};
  
  int i = 0;
  for (auto iter = sst3.begin(); iter != sst3.end(); ++iter, ++i) {
    const SSTRecord rec = *iter;
    CHECK(rec.key == keys[i]);
    CHECK(rec.offset == offsets[i]);
  } 
}


} // namespace

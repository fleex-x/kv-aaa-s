cmake_minimum_required(VERSION 3.0)

project(kv-aaa-s)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

file(GLOB zstd_common "libs/zstd/common/*.h" "libs/zstd/common/*.c")
file(GLOB zstd_compress "libs/zstd/compress/*.h" "libs/zstd/compress/*.c")
file(GLOB zstd_decompress "libs/zstd/decompress/*.h" "libs/zstd/decompress/*.c" "libs/zstd/decompress/*.S")
file(GLOB zstd_dict_builder "libs/zstd/dictBuilder/*.h" "libs/zstd/dictBuilder/*.c")

file(GLOB srcs "src/*.cpp")
file(GLOB tests "tests/*.cpp")
file(GLOB headers "include/*.h")
file(GLOB lib_headers "libs/include/*.h")
file(GLOB lib_cpp_srcs "libs/src/*.cpp")

set(zstd ${zstd_common} ${zstd_compress} ${zstd_decompress} ${zstd_dict_builder})

set(CMAKE_CXX_FLAGS "-Wall -Wextra -pedantic") # TODO add -Werror

include_directories(libs/include)
include_directories(include)

set_property(SOURCE libs/zstd/decompress/huf_decompress_amd64.S PROPERTY LANGUAGE C)

#add_executable(main main.cpp ${zstd} libs/zstd/zstd.h)

add_executable(test ${zstd} ${tests} ${srcs} ${headers} ${lib_headers} ${lib_cpp_srcs})

add_executable(sanitizer-test ${zstd} ${tests} ${srcs} ${headers} ${lib_headers} ${lib_cpp_srcs})
target_compile_options(sanitizer-test PUBLIC -fsanitize=address -fsanitize=undefined)
target_link_options(sanitizer-test PUBLIC -fsanitize=address -fsanitize=undefined)

add_executable(KVSRecordsTest tests/doctest_main.cpp tests/KVSRecords_test.cpp tests/doctest.h ${zstd} ${srcs} ${headers} ${lib_headers} ${lib_cpp_srcs})
add_executable(SkipListTest tests/doctest_main.cpp tests/skip_list_tests.cpp tests/doctest.h ${zstd} ${srcs} ${headers} ${lib_headers} ${lib_cpp_srcs})
add_executable(FileByteArrayTest tests/doctest_main.cpp tests/FileByteArray_tests.cpp tests/doctest.h ${zstd} ${srcs} ${headers} ${lib_headers} ${lib_cpp_srcs})
add_executable(SSTTest tests/doctest_main.cpp tests/sst.cpp tests/doctest.h ${zstd} ${srcs} ${headers} ${lib_headers} ${lib_cpp_srcs})
add_executable(LOGTest tests/doctest_main.cpp tests/log_test.cpp tests/doctest.h ${zstd} ${srcs} ${headers} ${lib_headers} ${lib_cpp_srcs})
add_executable(FileMemoryManagerTest tests/doctest_main.cpp tests/doctest.h tests/file_mm_test.cpp ${zstd} ${srcs} ${headers} ${lib_headers} ${lib_cpp_srcs})
add_executable(ShardTest tests/doctest_main.cpp tests/doctest.h tests/Shard_test.cpp ${zstd} ${srcs} ${headers} ${lib_headers} ${lib_cpp_srcs})

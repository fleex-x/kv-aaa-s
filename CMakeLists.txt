cmake_minimum_required(VERSION 3.10)

project(kv-aaa-s CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(include)

add_executable(main main.cpp)

add_executable(test doctest_main.cpp tests.cpp)
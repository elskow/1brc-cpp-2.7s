cmake_minimum_required(VERSION 3.28)
project(onebrc_cpp)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -flto -march=native -mtune=native")

add_executable(
        onebrc_cpp main.cpp
)
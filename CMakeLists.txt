cmake_minimum_required(VERSION 4.2)

project(TestTaskIPC LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)

add_executable(producer producer/main.cpp)
add_executable(consumer consumer/main.cpp)

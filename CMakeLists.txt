cmake_minimum_required(VERSION 3.20)

project(TestTaskIPC LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_package(ZLIB REQUIRED)
find_package(GTest)

add_library(common INTERFACE)
target_include_directories(common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/common)
target_link_libraries(common INTERFACE ZLIB::ZLIB)
target_compile_options(common INTERFACE -Wall -Wextra -Wpedantic)
target_compile_options(common INTERFACE $<$<CXX_COMPILER_ID:GNU>:-Winterference-size>)

add_executable(producer producer/main.cpp)
target_link_libraries(producer PRIVATE common)

add_executable(consumer consumer/main.cpp)
target_link_libraries(consumer PRIVATE common)

if(GTest_FOUND)
    enable_testing()
    add_executable(ring_test tests/ring_test.cpp)
    target_link_libraries(ring_test PRIVATE common GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(ring_test)
else()
    message(STATUS "GoogleTest not found, unit tests are not built")
endif()

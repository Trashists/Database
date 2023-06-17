# Cài đặt dự án
cmake_minimum_required(VERSION 3.18)
project(NhatMinhDatabase)

# Cài đặt trình biên dịch
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")

# Tìm đường dẫn của trình biên dịch
find_program(CMAKE_CXX_COMPILER g++)
find_program(CMAKE_C_COMPILER gcc)

# Định nghĩa đường dẫn thư mục include
include_directories(${PROJECT_SOURCE_DIR/include})

# Các thư mục chứa các file build riêng
add_subdirectory(src)
add_subdirectory(test/storageEngineTests)
# Thiết lập dự án
set(BINARY ${CMAKE_PROJECT_NAME})

# Source code
file(GLOB_RECURSE SOURCES LIST_DIRECTORIES true *.cpp)

# Phân tách main file để các module có thể chạy độc lập
set(SOURCES ${SOURCES})
list(REMOVE_ITEM SRC_FILES ${PROJECT_SOURCE_DIR}/src/main.cpp)
add_executable(${BINARY}_run ${SOURCES})

# Biên dịch code thành thư viện
add_library(${BINARY}_lib STATIC ${SOURCES})
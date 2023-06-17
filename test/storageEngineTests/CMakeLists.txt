# Khởi tạo
enable_testing()

# Thiết lập dự án
set(BINARY ${CMAKE_PROJECT_NAME}_test)

# Thêm file test
file(GLOB_RECURSE TEST_SOURCES LIST_DIRECTORIES false *.cpp)

# Chạy file test
add_executable(${BINARY} ${TEST_SOURCES})

add_test(NAME ${BINARY} COMMAND ${BINARY})
target_link_libraries(${BINARY} PUBLIC
    ${CMAKE_PROJECT_NAME}_lib
    gtest
    gtest_main
)
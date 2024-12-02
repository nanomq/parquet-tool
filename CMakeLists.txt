cmake_minimum_required(VERSION 2.6)
project(parquet-tool)

add_definitions(-std=c++17)

file(GLOB APP_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_executable(parquet-tool
	${APP_SOURCES})
target_link_libraries(parquet-tool boost_filesystem boost_thread boost_system boost_serialization pthread boost_chrono arrow parquet)


cmake_minimum_required(VERSION 2.6)
project(parquet-tool)

add_definitions(-std=c++17)

if (DEBUG)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
	if (ASAN)
		SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
	endif (ASAN)
endif (DEBUG)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/inc)
file(GLOB APP_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_executable(parquet-tool
	${APP_SOURCES})
target_link_libraries(parquet-tool boost_filesystem boost_thread boost_system boost_serialization pthread boost_chrono arrow parquet nng)


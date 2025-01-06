cmake_minimum_required(VERSION 2.6)
project(parquet-tool)

add_definitions(-std=c++17)

if (DEBUG)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
	if (ASAN)
		SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
	endif (ASAN)
endif (DEBUG)

set(DEPS
	boost_filesystem
	boost_thread
	boost_system
	boost_serialization
	pthread
	boost_chrono
	arrow
	parquet
	nng)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/inc)

set(SOURCES
	src/file.cpp
	src/log.cpp
	src/mqtt.cpp
	src/parquet.cpp)

add_library(parquet-tool-shared SHARED ${SOURCES})

add_executable(parquet-tool
	src/entry.cpp
	${SOURCES})
target_link_libraries(parquet-tool ${DEPS})

add_executable(test_parquet
	src/test_parquet.cpp
	${SOURCES})
target_link_libraries(test_parquet ${DEPS})


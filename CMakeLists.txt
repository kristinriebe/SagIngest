cmake_minimum_required (VERSION 2.6)
project (SagIngest.x)
set(AIDIR "${PROJECT_SOURCE_DIR}/SagIngest")


## Set compiler
# using following start of the command line for creating Makefile:
#CC=gcc-4.9 CXX=g++-4.9 cmake#

# set compiler here somewhere???
#CMAKE_CXX_COMPILER=g++-4.9
#CMAKE_C_COMPILER=gcc-4.9
# => no effects.

############################################################################
## SET THIS TO THE MYSQL BASE PATH IF IT IS NOT AT A STANDARD PLACE
############################################################################
set(MYSQL_BASE_PATH "/usr/local/mysql")

if(NOT DEFINED DBINGESTOR_LIBRARY_PATH)
       set(DBINGESTOR_LIBRARY_PATH "/usr/local/lib")
endif()

if(NOT DEFINED DBINGESTOR_INCLUDE_PATH)
       set(DBINGESTOR_INCLUDE_PATH "/usr/local/include")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/Modules/")

set(CMAKE_BUILD_TYPE Release)
set(SQLITE3_BUILD_IFFOUND 1)
set(MYSQL_BUILD_IFFOUND 1)
set(ODBC_BUILD_IFFOUND 1)

include_directories ("${PROJECT_SOURCE_DIR}/SagIngest")
include_directories ("${DBINGESTOR_INCLUDE_PATH}")
link_directories ("${DBINGESTOR_LIBRARY_PATH}")

if(MSVC)
set(CMAKE_CXX_FLAGS "/EHsc")
else()
#set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++0x")
endif()

file(GLOB FILES_SRC "${AIDIR}/*.h" "${AIDIR}/*.cpp")

#MESSAGE(STATUS "Dir: " ${DIDIR})

# Adjust the BOOST-path here, if needed
#SET(BOOST_ROOT /my/nice/path/to/boost_version_something)
#SET(CMAKE_INCLUDE_PATH ${CMAKE_INCLUDE_PATH} "/my/nice/path/to/boost_version_something")
#SET(Boost_NO_BOOST_CMAKE TRUE)
#message(STATUS "BOOST_ROOT: ${BOOST_ROOT}")

SET(Boost_USE_MULTITHREAD ON)
find_package (Boost COMPONENTS program_options filesystem system regex chrono serialization REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
message(STATUS "BOOST Include dirs: ${Boost_INCLUDE_DIRS}")
link_directories(${Boost_LIBRARY_DIRS})
message(STATUS "BOOST Lib dirs: ${Boost_LIBRARY_DIRS}")

#compiler
#//find_package (LARGEFILE64_SOURCE)
#//find_package (LARGEFILE_SOURCE)

#find_package (HDF5)
find_package (HDF5 COMPONENTS CXX REQUIRED)
message(STATUS "Found HDF5: ${HDF5_FOUND}")
include_directories(${HDF5_CXX_INCLUDE_DIRS})
link_directories(${HDF5_CXX_LIBRARY_DIRS})
add_definitions(-DHDF5_DEFINITIONS)
set (HDF5_libraries ${HDF5_hdf5_LIBRARY} ${HDF5_hdf5_cpp_LIBRARY})
set (HDF5_libraries hdf5 hdf5_cpp)

find_package (SQLITE3)
message(STATUS "Found SQLITE3: ${SQLITE3_FOUND}")
if(SQLITE3_FOUND AND SQLITE3_BUILD_IFFOUND)
	include_directories(${SQLITE3_INCLUDE_DIR})
	add_definitions(-DDB_SQLITE3)
endif()

find_package (Mysql)
message(STATUS "Found MySQL: ${MYSQL_FOUND}")
if(MYSQL_FOUND AND MYSQL_BUILD_IFFOUND)
	include_directories(${MYSQL_INCLUDE_DIR})
	add_definitions(-DDB_MYSQL)
endif()

find_package (ODBC)
message(STATUS "Found ODBC: ${ODBC_FOUND}")
if(ODBC_FOUND AND ODBC_BUILD_IFFOUND)
	include_directories(${ODBC_INCLUDE_DIRS})
	add_definitions(-DDB_ODBC)
endif()

add_executable (SagIngest.x ${FILES_SRC})

target_link_libraries(SagIngest.x ${Boost_LIBRARIES} ${HDF5_libraries} DBIngestor)

if(SQLITE3_FOUND)
        target_link_libraries(SagIngest.x ${SQLITE3_LIBRARIES})
endif()

if(MYSQL_FOUND)
        target_link_libraries(SagIngest.x ${MYSQL_LIBRARY})
endif()

if(ODBC_FOUND)
        target_link_libraries(SagIngest.x ${ODBC_LIBRARIES})
endif()


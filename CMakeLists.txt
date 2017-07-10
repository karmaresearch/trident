cmake_minimum_required (VERSION 2.8)
project (Trident)
include(ExternalProject)
include(FindPkgConfig)
set(CMAKE_MACOSX_RPATH 1)

if(COMMAND cmake_policy)
    cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

#Set compiler options
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(COMPILE_FLAGS "-c -DBOOST_LOG_DYN_LINK -fPIC -std=c++11 -Wall -Wno-sign-compare ${COMPILE_FLAGS}")
set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_C_COMPILER "gcc")

#Add support for multithreading ... Comment the following line to disable it
add_definitions(-DMT)

#Set to release mode if nothing is specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

#Add macPorts
# Detect if the "port" command is valid on this system; if so, return full path
EXECUTE_PROCESS(COMMAND which port RESULT_VARIABLE DETECT_MACPORTS OUTPUT_VARIABLE MACPORTS_PREFIX ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
IF (${DETECT_MACPORTS} EQUAL 0)
    # "/opt/local/bin/port" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local/bin" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local" is where MacPorts lives, add `/lib` suffix and link
    LINK_DIRECTORIES(${MACPORTS_PREFIX}/lib)
    INCLUDE_DIRECTORIES(${MACPORTS_PREFIX}/include)
ENDIF()

#Add homebrew
EXECUTE_PROCESS(COMMAND brew --prefix RESULT_VARIABLE exitstatus OUTPUT_VARIABLE BREW_PREFIX ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
IF (${exitstatus} EQUAL 0)
    LINK_DIRECTORIES(${BREW_PREFIX}/lib)
    INCLUDE_DIRECTORIES(${BREW_PREFIX}/include)
ENDIF()

#Python
pkg_check_modules(PY3 REQUIRED QUIET python3)
IF(PY3_FOUND)
    INCLUDE_DIRECTORIES(${PY3_INCLUDE_DIRS})
    LINK_DIRECTORIES(${PY3_LIBRARY_DIRS})
ENDIF()

#NumPY
if(NOT PYTHON_EXECUTABLE)
  if(NumPy_FIND_QUIETLY)
    find_package(PythonInterp 3 QUIET)
  else()
    find_package(PythonInterp 3)
    set(__numpy_out 1)
  endif()
endif()
if (PYTHON_EXECUTABLE)
  # Find out the include path
  execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" -c
            "from __future__ import print_function\ntry: import numpy; print(numpy.get_include(), end='')\nexcept:pass\n"
            OUTPUT_VARIABLE __numpy_path)
  # And the version
  execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" -c
            "from __future__ import print_function\ntry: import numpy; print(numpy.__version__, end='')\nexcept:pass\n"
    OUTPUT_VARIABLE __numpy_version)
elseif(__numpy_out)
  message(STATUS "Python executable not found.")
endif(PYTHON_EXECUTABLE)

find_path(PYTHON_NUMPY_INCLUDE_DIR numpy/arrayobject.h
  HINTS "${__numpy_path}" "${PYTHON_INCLUDE_PATH}" NO_DEFAULT_PATH)

if(PYTHON_NUMPY_INCLUDE_DIR)
  set(PYTHON_NUMPY_FOUND 1 CACHE INTERNAL "Python numpy found")
  include_directories(${PYTHON_NUMPY_INCLUDE_DIR} SYSTEM)
endif(PYTHON_NUMPY_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NumPy REQUIRED_VARS PYTHON_NUMPY_INCLUDE_DIR
VERSION_VAR __numpy_version)

#SPARSEHASH
find_path (sparsehash sparsehash/sparse_hash_map)
IF (${sparsehash} STREQUAL "sparsehash-NOTFOUND")
    message("Could not find sparsehash. I'm going to use the version from kognac ...")
    include_directories(${CMAKE_BINARY_DIR}/kognac/external/sparsehash/src/)
ENDIF()

#CURL
find_library(curl curl)

#LZ4
find_library(lz4 lz4)
find_path (lz4h lz4.h)
IF ((${lz4} STREQUAL "lz4-NOTFOUND") OR (${lz4h} STREQUAL "lz4h-NOTFOUND"))
    message("Could not find LZ4. I'm going to use the version from kognac")
    include_directories(${CMAKE_BINARY_DIR}/kognac/external/lz4/lib/)
    LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/kognac/external/lz4/lib)
    add_library(lz4 STATIC IMPORTED)
    set_property(TARGET lz4 PROPERTY IMPORTED_LOCATION kognac/external/lz4/lib/liblz4.a)
ELSE()
    include_directories(lz4h)
    message("Found LZ4")

ENDIF()

#TBB
find_library(tbb tbb PATHS ENV LD_LIBRARY_PATH)
find_path (tbbh tbb/task_scheduler_init.h PATHS ENV CPATH)
IF ((${tbb} STREQUAL "tbb-NOTFOUND") OR (${tbbh} STREQUAL "tbbh-NOTFOUND"))
    message(FATAL_ERROR "Could not find TBB (thread building blocks)")
ENDIF()

#Boost
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
FIND_PACKAGE(Boost COMPONENTS chrono thread system filesystem program_options log log_setup iostreams serialization REQUIRED)
IF (Boost_FOUND)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR} SYSTEM)
ENDIF()

#SNAP
add_library(snap STATIC IMPORTED)
IF(NOT EXISTS "${CMAKE_SOURCE_DIR}/snap/snap-core/libsnap.a")
    #Call make to compile the library
    message("Must compile SNAP first!")
    ExternalProject_Add(srcsnap
        SOURCE_DIR ${CMAKE_SOURCE_DIR}/snap/snap-core
        DOWNLOAD_DIR .
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND make lib
        INSTALL_COMMAND ""
        )
    add_dependencies(snap srcsnap)
ENDIF()
set_property(TARGET snap PROPERTY IMPORTED_LOCATION ../snap/snap-core/libsnap.a)
find_library(rt rt)
IF ((${rt} STREQUAL "rt-NOTFOUND"))
    set(rt "")
ENDIF()
find_package(OpenMP QUIET)
if (OPENMP_FOUND)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()
include_directories(SYSTEM snap/glib-core)
include_directories(SYSTEM snap)

#KOGNAC
IF(KOGNAC_LIB)
    message("Using KOGNAC provided installation: ${KOGNAC_LIB} ${KOGNAC_INC}")
    include_directories(${KOGNAC_INC})
    add_library(kognac STATIC IMPORTED)
    set_property(TARGET kognac PROPERTY IMPORTED_LOCATION ${KOGNAC_LIB})
ELSE()
    ExternalProject_Add(git-kognac
        DOWNLOAD_COMMAND git clone https://github.com/jrbn/kognac.git
        DOWNLOAD_DIR .
        SOURCE_DIR kognac
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND ""
        CMAKE_CACHE_ARGS -DBOOST_ROOT:string=${BOOST_ROOT} -DBoost_NO_BOOST_CMAKE:string=${Boost_NO_BOOST_CMAKE} -DBOOST_NO_SYSTEM_PATHS:string=${BOOST_NO_SYSTEM_PATHS}
        )
    ExternalProject_Get_Property(git-kognac SOURCE_DIR)
    include_directories(${SOURCE_DIR}/include)
    add_library(kognac STATIC IMPORTED)
    add_dependencies(kognac git-kognac)
    set_property(TARGET kognac PROPERTY IMPORTED_LOCATION kognac/libkognac.a)
ENDIF()

#add files to compile
file(GLOB trident_SRC
    "src/trident/**/*.cpp" "src/layers/*.cpp" "rdf3x/src/**/*.cpp"  src/snap/*.cpp src/trident/mining/miner.cpp
    )
file(GLOB tridentp_SRC
    "src/python/*.cpp")
include_directories(include/ rdf3x/include)

#Create both a library and the executable program
add_library(trident STATIC ${trident_SRC})
add_library(tridentp SHARED ${tridentp_SRC}) #Python library
set_target_properties(tridentp PROPERTIES OUTPUT_NAME "trident" PREFIX "" SUFFIX ".so")
add_executable(trident_exec src/client/main.cpp src/client/main_params.cpp)
set_target_properties(trident PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
set_target_properties(tridentp PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
set_target_properties(trident_exec PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")

#Add last dependencies that need trident
#PTHREADS
find_package(Threads REQUIRED)
if (THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(PUBLIC trident "-pthread")
endif()
if (CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(trident "${CMAKE_THREAD_LIBS_INIT}")
endif()

#Create both a library and the executable program
TARGET_LINK_LIBRARIES(trident curl kognac lz4 tbb snap ${rt} ${Boost_LIBRARIES})
TARGET_LINK_LIBRARIES(tridentp trident ${PY3_LIBRARIES})
TARGET_LINK_LIBRARIES(trident_exec trident)

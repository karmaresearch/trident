#Add support for multithreading ... Comment the following line to disable it
#-DMT=1 to enable multithreading support
#-DPYTHON=1 enable python support
#-DSERVER=1 to start the webserver
#-DANALYTICS=1 enable analytics support
#-DSPARQL=1 enable advanced sparql support
#-DML=1 enable machine learning algorithms

cmake_minimum_required (VERSION 2.8)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_C_COMPILER "gcc")

project (Trident)
include(ExternalProject)
set(CMAKE_MACOSX_RPATH 1)
set(CMAKE_VS_PLATFORM_NAME, "x64")

if(COMMAND cmake_policy)
    cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

IF (JAVA)
    find_package(Java REQUIRED)
    include(UseJava)
    find_package(JNI REQUIRED)
    include_directories(${JNI_INCLUDE_DIRS})
ENDIF()

if(SERVER)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DSERVER=1")
    message("SERVER forces the enabling of SPARQL")
    set(SPARQL "1")
ENDIF()
IF(SPARQL)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DSPARQL=1")
ENDIF()
IF(PYTHON)
    message("PYTHON forces the enabling of ML and ANALYTICS")
    set(ML "1")
    set(ANALYTICS "1")
ENDIF()
if(ANALYTICS)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DANALYTICS=1")
ENDIF()
IF(ML)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DML=1")
ENDIF()
IF(MT)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DMT=1")
ENDIF()

#Set compiler options
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -DDEBUG")
    set(COMPILE_FLAGS "-c -fPIC -std=c++11 -Wall -Wno-sign-compare ${COMPILE_FLAGS}")
endif()

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

#ZLIB
find_package(ZLIB REQUIRED)

#KOGNAC
IF(KOGNAC_LIB)
    message("Using KOGNAC provided installation: ${KOGNAC_LIB} ${KOGNAC_INC}")
    include_directories(${KOGNAC_INC})
    add_library(kognac-core SHARED IMPORTED)
ELSE()
    ExternalProject_Add(git-kognac
        DOWNLOAD_COMMAND git clone https://github.com/karmaresearch/kognac.git
        DOWNLOAD_DIR .
        SOURCE_DIR kognac
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND ""
        )
    ExternalProject_Get_Property(git-kognac SOURCE_DIR)
    include_directories(${SOURCE_DIR}/include)
    add_library(kognac-core SHARED IMPORTED)
    add_dependencies(kognac-core git-kognac)
    set(KOGNAC_LIB "${CMAKE_BINARY_DIR}/kognac")
ENDIF()
set_property(TARGET kognac-core PROPERTY IMPORTED_LOCATION "${KOGNAC_LIB}/${CMAKE_SHARED_LIBRARY_PREFIX}kognac-core${CMAKE_SHARED_LIBRARY_SUFFIX}")
LINK_DIRECTORIES("${KOGNAC_LIB}")

#SPARSEHASH
find_path (sparsehash sparsehash/sparse_hash_map)
IF (${sparsehash} STREQUAL "sparsehash-NOTFOUND")
    message("Could not find sparsehash. I'm going to use the version from kognac ...")
    include_directories(${KOGNAC_LIB}/external/sparsehash/src/)
ENDIF()

#LZ4
# find_library(lz4 lz4)
# find_path (lz4h lz4.h)
# IF ((${lz4h} STREQUAL "lz4h-NOTFOUND"))
#     message("Could not find LZ4. I'm going to use the version from kognac")
    message("using the lz4 version from kognac")
    set(LZ4_LIB "${KOGNAC_LIB}/external/lz4/lib")
    include_directories(${LZ4_LIB})
    LINK_DIRECTORIES(${LZ4_LIB})
    add_library(lz4 STATIC IMPORTED)
    set_property(TARGET lz4 PROPERTY IMPORTED_LOCATION "${LZ4_LIB}/${CMAKE_STATIC_LIBRARY_PREFIX}lz4${CMAKE_STATIC_LIBRARY_SUFFIX}")
# ELSE()
# include_directories(lz4h)
# message("-- Found LZ4")
# ENDIF()

#Create the core library
include_directories(include/ rdf3x/include)
file(GLOB trident_SRC
    "src/trident/binarytables/*.cpp"
    "src/trident/files/*.cpp"
    "src/trident/iterators/*.cpp"
    "src/trident/kb/*.cpp"
    "src/trident/model/*.cpp"
    "src/trident/tests/*.cpp"
    "src/trident/tree/*.cpp"
    "src/trident/utils/*.cpp"
    )
add_library(trident-core SHARED ${trident_SRC})
set_target_properties(trident-core PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
#PTHREADS
find_package(Threads REQUIRED)
if (THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(PUBLIC trident-core "-pthread")
endif()
TARGET_LINK_LIBRARIES(trident-core ${ZLIB_LIBRARIES} kognac-core lz4)
if (CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(trident-core "${CMAKE_THREAD_LIBS_INIT}")
endif()
#Create the main executable
add_executable(trident src/client/main.cpp src/client/main_params.cpp)
set_target_properties(trident PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
TARGET_LINK_LIBRARIES(trident trident-core)

#SPARQL
IF(SPARQL)
    file(GLOB sparql_SRC
        "src/trident/sparql/*.cpp"
        "src/layers/*.cpp"
        "rdf3x/src/**/*.cpp"
        "rdf3x/src/**/**/*.cpp"
        "rdf3x/src/*.cpp")
    add_library(trident-sparql SHARED ${sparql_SRC})
    set_target_properties(trident-sparql PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
    TARGET_LINK_LIBRARIES(trident-sparql kognac-core trident-core)
    TARGET_LINK_LIBRARIES(trident trident-sparql)
ENDIF()

#ML
IF(ML)
    file(GLOB ml_SRC
        "src/trident/mining/*.cpp"
        "src/trident/ml/*.cpp"
        src/trident/mining/miner.cpp)

    # Install FAISS
    find_library(omp omp)
    find_library(openblas openblas)
    ExternalProject_Add(git-faiss
        DOWNLOAD_COMMAND git clone https://github.com/facebookresearch/faiss.git
        DOWNLOAD_DIR .
        SOURCE_DIR faiss/
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND "export" "CXX=/usr/local/opt/llvm/bin/clang -std=c++11" && "export" "CPUFLAGS=-msse4 -mpopcnt" && "export" "" && "export" "LDFLAGS=-fPIC -fopenmp -L/usr/local/opt/llvm/lib" && "export" "CPPFLAGS=-fPIC -m64 -Wall -g -O3 -Wno-sign-compare -DFINTEGER=int -fopenmp -I/usr/local/opt/llvm/include" && make -j libfaiss.a
        INSTALL_COMMAND ""
        )
    ExternalProject_Get_Property(git-faiss SOURCE_DIR)
    include_directories(${SOURCE_DIR}/..)
    add_library(faiss STATIC IMPORTED)
    add_dependencies(faiss git-faiss)
    set(FAISS_LIB "${CMAKE_BINARY_DIR}/faiss")
    set_property(TARGET faiss PROPERTY IMPORTED_LOCATION "${FAISS_LIB}/libfaiss.a")

    set(COMPILE_FLAGS "-DFINTEGER=int -m64 -msse4 ${COMPILE_FLAGS}")
    add_library(trident-ml SHARED ${ml_SRC})
    set_target_properties(trident-ml PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
    TARGET_LINK_LIBRARIES(trident-ml kognac-core trident-core faiss omp ${openblas})
    TARGET_LINK_LIBRARIES(trident trident-ml)
ENDIF()

#ANALYTICS
if (ANALYTICS)
    add_library(snap STATIC IMPORTED)
    IF(NOT EXISTS "${CMAKE_SOURCE_DIR}/snap/snap-core/libsnap.a")
        #Call make to compile the library
        message("I'm also going to compile SNAP")
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

    file(GLOB analytics_SRC
        src/snap/*.cpp)

    add_library(trident-ana SHARED ${analytics_SRC})
    set_target_properties(trident-ana PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
    TARGET_LINK_LIBRARIES(trident-ana kognac-core trident-core snap ${rt})
    TARGET_LINK_LIBRARIES(trident trident-ana)
ENDIF()

IF(JAVA)
    file(GLOB trident_javaSRC "src/trident/java/native/*.cpp")
    add_library(trident-java SHARED ${trident_javaSRC})
    set_target_properties(trident-java PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
ENDIF()

#WEBSERVER
IF (SERVER)
    file(GLOB webserver_SRC
        "src/trident/server/*.cpp")
    #CURL
    #find_library(curl curl)

    add_library(trident-web SHARED ${webserver_SRC})
    set_target_properties(trident-web PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
    TARGET_LINK_LIBRARIES(trident-web kognac-core trident-core trident-sparql)
    TARGET_LINK_LIBRARIES(trident trident-web)
ENDIF()

#Create Python library
IF(PYTHON)
    include(FindPkgConfig)
    set(Python_ADDITIONAL_VERSIONS 3.5)
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

    file(GLOB tridentp_SRC "src/python/*.cpp")
    add_library(tridentp SHARED ${tridentp_SRC}) #Python library
    set_target_properties(tridentp PROPERTIES OUTPUT_NAME "trident" PREFIX "" SUFFIX ".so")
    set_target_properties(tridentp PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
    TARGET_LINK_LIBRARIES(tridentp trident-core trident-ml trident-ana ${PY3_LIBRARIES})
ENDIF()

if(JAVA)
    file(GLOB javaSRC "src/trident/java/karmaresearch/trident/*.java")
    Create_javadoc(trident PACKAGES karmaresearch.trident
        SOURCEPATH ${CMAKE_CURRENT_SOURCE_DIR}/src/trident/java WINDOWTITLE "Trident JNI Interface")
    TARGET_LINK_LIBRARIES(trident-java trident-core trident-sparql trident-web kognac-core)
    add_jar(jtrident ${javaSRC} trident-java trident-core trident-sparql kognac-core GENERATE_NATIVE_HEADERS headers DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/src/trident/java/native/)
    add_dependencies(jtrident trident-java)
ENDIF()


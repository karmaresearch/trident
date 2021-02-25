/*
 * Copyright 2017 Jacopo Urbani
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
**/


#ifndef CONSTS_H_
#define CONSTS_H_

#include <inttypes.h>

#if defined(_WIN32)
    #define DDLIMPORT __declspec(dllimport)
    #define DDLEXPORT __declspec(dllexport)
    #if TRIDENT_SHARED_LIB
        #define LIBEXP DDLEXPORT
    #else
        #define LIBEXP DDLIMPORT
    #endif
#else
    #define LIBEXP
    #define DDLIMPORT
    #define DDLEXPORT
#endif

//This is a list of all permutation IDs. It is replicated in tridentcompr/Compressor.h
// Use IDX_SPO : to find all possible Os where S and P are fixed
// Use IDX_POS : to find all possible Ss where O and P are fixed
#define IDX_SPO 0
#define IDX_OPS 1
#define IDX_POS 2
#define IDX_SOP 3
#define IDX_OSP 4
#define IDX_PSO 5

#define ROW_ITR 0
#define CLUSTER_ITR 1
#define COLUMN_ITR 2
#define NEWCOLUMN_ITR 3
#define NEWROW_ITR 4
#define NEWCLUSTER_ITR 5
#define ARRAY_ITR 6
#define CACHE_ITR 7
#define AGGR_ITR 8
#define SCAN_ITR 9
#define DIFFSCAN_ITR 10
#define EMPTY_ITR 11
#define TERM_ITR 12
#define COMPOSITE_ITR 13
#define COMPOSITETERM_ITR 14
#define DIFFTERM_ITR 15
#define COMPOSITESCAN_ITR 16
#define DIFF1_ITR 17
#define RM_ITR 18
#define RMCOMPOSITETERM_ITR 19
#define REORDER_ITR 20
#define REORDERTERM_ITR 21

//Use for dynamic layout
#define W_DIFFERENCE 0
#define WO_DIFFERENCE 1
#define COMPR_1 0
#define COMPR_2 1
#define NO_COMPR 2

//Size buffer (i.e. number of elements) to sort during the compression
#define SORTING_BLOCK_SIZE 25000000

#define MAX_N_BLOCKS_IN_CACHE 1000000

//Used in the dictionary lookup thread
#define OUTPUT_BUFFER_SIZE 2048
#define MAX_N_PATTERNS 10

//StringBuffer block size
#define SB_BLOCK_SIZE 65536

//Generic options
#define N_PARTITIONS 6
#define THRESHOLD_KEEP_MEMORY 1000*1024

#define MAX_N_FILES 4096

//Used in the cache of the tree to serialize the nodes
#define SIZE_SUPPORT_BUFFER 512 * 1024

#define MAX_SESSIONS 1024
#define NO_BLOCK_SESSION -1
#define EMPTY_SESSION -2
#define FREE_SESSION -3

//Size indices in the binary tables
#define ADDITIONAL_SECOND_INDEX_SIZE 512
#define FIRST_INDEX_SIZE 256

#endif

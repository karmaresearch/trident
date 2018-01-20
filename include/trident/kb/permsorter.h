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


#ifndef _PERM_SORTER_H
#define _PERM_SORTER_H

#include <kognac/multidisklz4reader.h>
#include <kognac/multidisklz4writer.h>

#include <string>
#include <vector>

class PermSorter {
    private:
        static void writeTermInBuffer(char *buffer, const long n);

        static void sortChunks_seq_old(const int idReader,
                MultiDiskLZ4Reader *reader,
                char *start,
                char *end,
                long *count);

        static void sortChunks_seq(const int idReader,
                MultiDiskLZ4Reader *reader,
                std::vector<std::unique_ptr<char[]>> *rawTriples,
                long start,
                long end,
                long *count,
                std::vector<std::pair<string, char>> additionalPermutations,
                bool outputSPO);

        static void dumpPermutation_seq_old(
                char *rawinput,
                long *start,
                long *end,
                MultiDiskLZ4Writer *currentWriter,
                int currentPart,
                int sorter);

        static void dumpPermutation_seq(
                char *start,
                char *end,
                MultiDiskLZ4Writer *currentWriter,
                int currentPart);

        static void dumpPermutation_old(char *input, long end,
                int parallelProcesses,
                int maxReadingThreads,
                string outputFile,
                std::vector<long> &idx,
                int sorter);

        static void dumpPermutation(char *input, long end,
                int parallelProcesses,
                int maxReadingThreads,
                string outputFile);

        static bool isMax(char *input, long idx);

        static void sortPermutation_old(char *rawinput,
                std::vector<long> *idx,
                int sorter,
                int nthreads);

        static void sortPermutation(char *start,
                char *end, int nthreads);

    public:
        static void sortChunks_Old(string inputdir,
                int maxReadingThreads,
                int parallelProcesses,
                long estimatedSize,
                std::vector<std::pair<string, char>> &additionalPermutations);

        static void sortChunks(string inputdir,
                int maxReadingThreads,
                int parallelProcesses,
                long estimatedSize,
                bool outputSPO,
                std::vector<std::pair<string, char>> &additionalPermutations);

        static long readTermFromBuffer(char *buffer);
};
#endif

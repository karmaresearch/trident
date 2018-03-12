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
        static void writeTermInBuffer(char *buffer, const int64_t n);

        static void sortChunks_seq(const int idReader,
                MultiDiskLZ4Reader *reader,
                std::vector<std::unique_ptr<char[]>> *rawTriples,
                int64_t start,
                int64_t end,
                int64_t *count,
                std::vector<std::pair<string, char>> additionalPermutations,
                bool outputSPO);

        static void dumpPermutation_seq(
                char *start,
                char *end,
                MultiDiskLZ4Writer *currentWriter,
                int currentPart);

        static void dumpPermutation(char *input, int64_t end,
                int parallelProcesses,
                int maxReadingThreads,
                string outputFile);

        static bool isMax(char *input, int64_t idx);

        static void sortPermutation(char *start,
                char *end, int nthreads);

    public:
        static void sortChunks(string inputdir,
                int maxReadingThreads,
                int parallelProcesses,
                int64_t estimatedSize,
                bool outputSPO,
                std::vector<std::pair<string, char>> &additionalPermutations);

        static int64_t readTermFromBuffer(char *buffer);
};
#endif

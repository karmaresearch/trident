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


#ifndef MEMORYOPT_H_
#define MEMORYOPT_H_

#include <trident/kb/kbconfig.h>

class MemoryOptimizer {

private:

    static int calculateBytesPerDictPair(long nTerms, int leafSize);

public:
    static void optimizeForWriting(long inputTriples, KBConfig &config);

    static void optimizeForReading(int ndicts, KBConfig &config);

    static void optimizeForReasoning(int ndicts, KBConfig &config);

};

#endif /* MEMORYOPT_H_ */

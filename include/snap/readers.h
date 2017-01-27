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


#ifndef _READERS_H
#define _READERS_H

#include <trident/binarytables/factorytables.h>

#include <vector>

class KB;
class SnapReaders {
    public:
        typedef long (*pReader)(const char*, const long);
        const static pReader readers[256];
        static std::vector<const char*> f_sop;
        static std::vector<const char*> f_osp;
        static void loadAllFiles(KB *kb);
};

#endif

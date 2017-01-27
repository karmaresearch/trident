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


#ifndef _BINARYTABLEREADERS_H
#define _BINARYTABLEREADERS_H

#include <inttypes.h>

class ByteReader {
public:
    static char size() {
        return 1;
    }

    static uint64_t read(const char* buffer) {
        return (*buffer) & 0xFF;
    }
};

class ShortReader {
public:
    static char size() {
        return 2;
    }

    static uint64_t read(const char* buffer) {
        return (uint16_t) (((buffer[0] & 0xFF) << 8) + (buffer[1] & 0xFF));
    }
};

class IntReader {
public:
    static char size() {
        return 4;
    }

    static uint64_t read(const char* buffer) {
        uint64_t n = (long)(buffer[0] & 0xFF) << 24;
        n += (buffer[1] & 0xFF) << 16;
        n += (buffer[2] & 0xFF) << 8;
        n += buffer[3] & 0xFF;
        return n;

        //return *((int*)buffer);
    }
};

class LongIntReader {
public:
    static char size() {
        return 5;
    }

    static uint64_t read(const char* buffer) {
        uint64_t n = (long) (buffer[0]) << 32;
        n += (long) (buffer[1] & 0xFF) << 24;
        n += (buffer[2] & 0xFF) << 16;
        n += (buffer[3] & 0xFF) << 8;
        n += buffer[4] & 0xFF;
        return n;
    }
};

class LongReader {
public:
    static char size() {
        return 8;
    }

    static uint64_t read(const char* buffer) {
        uint64_t n = (long) (buffer[0]) << 56;
        n += (long) (buffer[1] & 0xFF) << 48;
        n += (long) (buffer[2] & 0xFF) << 40;
        n += (long) (buffer[3] & 0xFF) << 32;
        n += (long) (buffer[4] & 0xFF) << 24;
        n += (buffer[5] & 0xFF) << 16;
        n += (buffer[6] & 0xFF) << 8;
        n += buffer[7] & 0xFF;
        return n;
    }
};
#endif

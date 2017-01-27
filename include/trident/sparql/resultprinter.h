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


#ifndef _OUTPUTQUERY_H
#define _OUTPUTQUERY_H

#include <trident/kb/consts.h>

class OutputBuffer {

    long buffer1[OUTPUT_BUFFER_SIZE];
    long buffer2[OUTPUT_BUFFER_SIZE];

    long *currentBuffer;
    bool firstBufferActive;
    int currentSize;

public:
    OutputBuffer() {
        currentBuffer = buffer1;
        firstBufferActive = true;
        currentSize = 0;
    }

    void addTerm(long t) {
        currentBuffer[currentSize++] = t;
    }

    long *getBuffer() {
        long *o = currentBuffer;
        if (firstBufferActive) {
            currentBuffer = buffer2;
        } else {
            currentBuffer = buffer1;
        }
        firstBufferActive = !firstBufferActive;
        currentSize = 0;
        return o;
    }
};

class ResultPrinter {
private:

    void printHeader();

    void printNumericRow(long *row, int size);

    void printFooter();

public:


};

#endif

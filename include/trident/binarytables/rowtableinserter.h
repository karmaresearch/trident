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


#ifndef _ROWTABLEINSERTER_H
#define _ROWTABLEINSERTER_H

#include <trident/kb/consts.h>
#include <trident/binarytables/binarytableinserter.h>

class RowTableInserter: public BinaryTableInserter {
private:
    int comprValue1;
    int comprValue2;
    int diffValue1;
    int64_t previousValue1;
    int64_t nElements;

    void writeFirstTerm(int64_t t1);
    void writeSecondTerm(int64_t t2);

public:

    int getType() {
        return ROW_ITR;
    }

    void setCompressionMode(int v1, int v2);

    void setDifferenceMode(int d1);

    void startAppend();

    void append(int64_t t1, int64_t t2);

    void stopAppend();

};
#endif

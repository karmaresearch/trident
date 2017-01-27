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


#ifndef _NEWROWTABLEINSERTER_H
#define _NEWROWTABLEINSERTER_H

#include <trident/kb/consts.h>
#include <trident/binarytables/binarytableinserter.h>

class NewRowTableInserter: public BinaryTableInserter {
private:
    char reader1, reader2;

    void writeFirstTerm(long t1);
    void writeSecondTerm(long t2);

public:

    int getType() {
        return NEWROW_ITR;
    }

    void setReaderSizes(char reader1, char reader2) {
        this->reader1 = reader1;
        this->reader2 = reader2;
    }

    void startAppend();

    void append(long t1, long t2);

    void stopAppend();

};
#endif

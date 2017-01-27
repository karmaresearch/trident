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


#ifndef _NEWCLUSTERTABLEINSERTER_H
#define _NEWCLUSTERTABLEINSERTER_H

#include <trident/kb/consts.h>
#include <trident/binarytables/binarytableinserter.h>

class NewClusterTableInserter: public BinaryTableInserter {
private:
    char reader1, reader2, countsize;

    long prevt1;
    std::vector<long> v2;

    void writeFirstTerm(long t1);
    void writeSecondTerm(long t2);

    void writeGroup();

public:

    int getType() {
        return NEWCLUSTER_ITR;
    }

    void setSizes(char reader1, char reader2, char countsize) {
        this->reader1 = reader1;
        this->reader2 = reader2;
        this->countsize = countsize;
    }

    void startAppend();

    void append(long t1, long t2);

    void stopAppend();

};
#endif

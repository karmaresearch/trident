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


#ifndef _NEW_ROWTABLE_H
#define _NEW_ROWTABLE_H

#include <trident/binarytables/newtable.h>
#include <trident/kb/consts.h>

#include <iostream>
#include <assert.h>

using namespace std;

template<class Reader1, class Reader2>
class NewRowTable: public AbsNewTable {
    private:
        const char *start;
        const char *current;
        const char *end;

        long currentValue1, currentValue2;
        long count;
        bool isSecondColumnIgnored;

        const char *currentMark;

    public:
        long getValue1() {
            return currentValue1;
        }

        long getValue2() {
            return currentValue2;
        }

        virtual bool isConstrained() const {
            return false;
        }

        char getReaderSize1() const {
            return Reader1::size();
        }

        char getReaderSize2() const {
            return Reader2::size();
        }

        char getReaderCountSize() const {
            return 0;
        }

        void clear() {
        }

        uint64_t getCardinality() {
            if (isSecondColumnIgnored) {
                long count = 0;
                if (currentValue1 != -1)
                    count++;
                long prevEl = currentValue1;
                while (current != end) {
                    currentValue1 = Reader1::read(current);
                    current += Reader1::size() + Reader2::size();
                    if (currentValue1 != prevEl) {
                        count++;
                        prevEl = currentValue1;
                    }
                }
                return count;
            } else {
                return (end - start) / (Reader1::size() + Reader2::size());
            }
        }

        uint64_t estCardinality() {
            int space = end - current;
            int els = space / (Reader1::size() + Reader2::size());
            if (els == 0)
                els = 1;
            return els;
        }

        void next() {
            // Record the previous values and coordinates
            if (isSecondColumnIgnored) {
                currentValue1 = Reader1::read(current);
                count = 1;

                //move to the next entry
                current += Reader1::size() + Reader2::size();
                while (current != end && Reader1::read(current) == currentValue1) {
                    current += Reader1::size() + Reader2::size();
                    count++;
                }

            } else {
                currentValue1 = Reader1::read(current);
                current += Reader1::size();
                currentValue2 = Reader2::read(current);
                current += Reader2::size();
            }
        }

        bool next(long &v1, long &v2, long &v3) {
            // Record the previous values and coordinates
            if (isSecondColumnIgnored) {
                currentValue1 = Reader1::read(current);
                v1 = key;
                v2 = currentValue1;
                count = 1;

                //Is there a next?
                current += Reader1::size() + Reader2::size();
                while (current != end && Reader1::read(current) == currentValue1) {
                    current += Reader1::size() + Reader2::size();
                    count++;
                }
            } else {
                currentValue1 = Reader1::read(current);
                current += Reader1::size();
                currentValue2 = Reader2::read(current);
                current += Reader2::size();
                v1 = key;
                v2 = currentValue1;
                v3 = currentValue2;
            }
            return current < end;
        }

        bool hasNext() {
            assert(current <= end);
            return current < end;
        }

        void setup(const char* start, const char *end) {
            initializeConstraints();
            this->start = start;
            current = start;
            this->end = end;
            isSecondColumnIgnored = false;
            currentValue1 = currentValue2 = -1;
            count = 1;
        }

        void setup(long c1, const char* s, const char *e) {
            //Read the interval
            long nrows = (e - s) / (Reader1::size() + Reader2::size());
            if (nrows > 64) {
                //Do a binary search
                const char *startS = s;
                const char *endS = e;
                const uint8_t blocksize = Reader1::size() + Reader2::size();
                while (nrows > 64) {
                    //Restrict the range
                    long gap = (nrows / 2) * blocksize;
                    assert(gap >= 0);
                    const char *middle = startS + gap;
                    long middlevalue = Reader1::read(middle);
                    if (middlevalue < c1) {
                        startS = middle;
                    } else if (middlevalue >= c1) {
                        endS = middle - blocksize;
                    }
                    nrows = (endS - startS) / (Reader1::size() + Reader2::size());
                    assert(nrows >= 0);
                }
                while (startS > s && Reader1::read(startS) == c1) {
                    startS -= blocksize;
                }
                s = startS;
            }
            long v = Reader1::read(s);
            while (v < c1 && (s + Reader1::size() + Reader2::size()) < e) {
                s += Reader1::size() + Reader2::size();
                v = Reader1::read(s);
            }
            if (v != c1) {
                setup(s, s);
            } else {
                //Now I must fix end
                const char *newend = s + Reader1::size() + Reader2::size();
                while (newend < e) {
                    v = Reader1::read(newend);
                    if (v != c1)
                        break;
                    newend += Reader1::size() + Reader2::size();
                }
                setup(s, newend);
            }
        }

        void setup(long c1, long c2, const char* s, const char *e) {
            //Read the interval
            long nrows = (e - s) / (Reader1::size() + Reader2::size());
            if (nrows > 64) {
                //Do a binary search
                const char *startS = s;
                const char *endS = e;
                const uint8_t blocksize = Reader1::size() + Reader2::size();
                while (nrows > 64) {
                    //Restrict the range
                    long gap = (nrows / 2) * blocksize;
                    assert(gap >= 0);
                    const char *middle = startS + gap;
                    long middlevalue = Reader1::read(middle);
                    if (middlevalue < c1) {
                        startS = middle;
                    } else if (middlevalue >= c1) {
                        endS = middle - blocksize;
                    }
                    nrows = (endS - startS) / (Reader1::size() + Reader2::size());
                    assert(nrows >= 0);
                }
                while (startS > s && Reader1::read(startS) == c1) {
                    startS -= blocksize;
                }
                s = startS;
            }
            long v1 = Reader1::read(s);
            long v2 = Reader2::read(s + Reader1::size());
            while ((v1 < c1 || (v1 == c1 && v2 < c2)) && s < e) {
                s += Reader1::size() + Reader2::size();
                v1 = Reader1::read(s);
                v2 = Reader2::read(s + Reader1::size());
            }
            if (v1 == c1 && v2 == c2 && s < e) {
                setup(s, s + Reader1::size() + Reader2::size());
            } else {
                setup(s, s);
            }
        }

        void first() {
            next();
        }

        void moveto(const long c1, const long c2) {
            assert(currentValue1 != -1);
            assert(isSecondColumnIgnored || currentValue2 != -1);
            if (!hasNext() && (c1 > currentValue1 ||
                        (!isSecondColumnIgnored && c1 == currentValue1 && c2 > currentValue2))) {
                return;
            }

            if (c1 > currentValue1 ||
                    (!isSecondColumnIgnored && c1 == currentValue1 && c2 > currentValue2)) {
                // Read the first entry
                long currentvalue = -1;
                while (current != end) {
                    currentvalue = Reader1::read(current);
                    if (currentvalue >= c1)
                        break;
                    current += Reader1::size() + Reader2::size();
                }
                assert(current <= end);

                if (c2 > 0 && current != end && currentvalue == c1) {
                    long currentC1 = Reader1::read(current);
                    while (current != end && Reader1::read(current) == currentC1 &&
                            Reader2::read(current + Reader1::size()) < c2) {
                        current += Reader1::size() + Reader2::size();
                    }
                }
            } else {
                current -= Reader1::size() + Reader2::size();
            }

        }

        void mark() {
            currentMark = current;
        }

        void reset(const char i) {
            current = currentMark;
        }

        void ignoreSecondColumn() {
            isSecondColumnIgnored = true;
            if (currentValue1 != -1) {
                //I already read some data. I move to the next entry
                while (current < end && Reader1::read(current) == currentValue1) {
                    current += Reader1::size() + Reader2::size();
                    count++;
                }
            }
        }

        long getCount() {
            return count;
        }

        int getTypeItr() {
            return NEWROW_ITR;
        }

        static long s_getValue1AtRow(const char *start, const long rowId) {
            const char *pos = start + (Reader1::size() + Reader2::size()) * rowId;
            return Reader1::read(pos);
        }

        static void s_getValue12AtRow(const uint64_t sizetable,
                const uint8_t offset,
                const char *start, const uint64_t rowId,
                uint64_t &v1, uint64_t &v2) {
            const char *pos = start + (Reader1::size() + Reader2::size()) * rowId;
            v1 = Reader1::read(pos);
            v2 = Reader2::read(pos + Reader1::size());
        }
};

#endif

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


#ifndef _NEW_CLUSTERTABLE_H
#define _NEW_CLUSTERTABLE_H

#include <trident/binarytables/newtable.h>
#include <trident/kb/consts.h>

#include <iostream>
#include <assert.h>

using namespace std;

template<class Reader1, class Reader2, class ReaderCount>
class NewClusterTable: public AbsNewTable {
    private:
        const char *start;
        const char *current;
        const char *end;

        long currentValue1, currentValue2;
        long count, countgroup;
        bool isSecondColumnIgnored;

        const char *m_current;
        long m_currentValue1, m_currentValue2;
        long m_count;
        long m_countgroup;

    public:
        long getValue1() {
            return currentValue1;
        }

        long getValue2() {
            return currentValue2;
        }

        char getReaderSize1() const {
            return Reader1::size();
        }

        char getReaderSize2() const {
            return Reader2::size();
        }

        char getReaderCountSize() const {
            return ReaderCount::size();
        }

        void clear() {
        }

        uint64_t getCardinality() {
            //Read all elements
            long sum = 0;
            long nels = 0;
            const char *s = start;
            while (s != end) {
                //Skip first el
                s += Reader1::size();
                nels++;
                count = countgroup = ReaderCount::read(s);
                sum += count;
                s += ReaderCount::size() + Reader2::size() * count;
            }
            if (isSecondColumnIgnored) {
                return nels;
            } else {
                return sum;
            }
        }

        uint64_t estCardinality() {
            int space = end - current;
            int els = space /
                (Reader1::size() +
                 Reader2::size() +
                 ReaderCount::size()) * 2;
            if (els == 0)
                els = 1;
            return els;
        }

        void next() {
            // Record the previous values and coordinates
            if (isSecondColumnIgnored) {
                currentValue1 = Reader1::read(current);
                current += Reader1::size();
                count = countgroup = ReaderCount::read(current);

                //move to the next entry
                current += ReaderCount::size() + Reader2::size() * count;
            } else {
                if (count > 0) {
                    currentValue2 = Reader2::read(current);
                    current += Reader2::size();
                } else {
                    currentValue1 = Reader1::read(current);
                    current += Reader1::size();
                    count = countgroup = ReaderCount::read(current);
                    current += ReaderCount::size();
                    currentValue2 = Reader2::read(current);
                    current += Reader2::size();
                }
                count--;
            }
        }

        bool next(long &v1, long &v2, long &v3) {
            // Record the previous values and coordinates
            if (isSecondColumnIgnored) {
                currentValue1 = Reader1::read(current);
                current += Reader1::size();
                count = countgroup = ReaderCount::read(current);
                current += ReaderCount::size() + count * Reader2::size();

                v1 = key;
                v2 = currentValue1;
            } else {
                if (count == 0) {
                    currentValue1 = Reader1::read(current);
                    current += Reader1::size();
                    count = countgroup = ReaderCount::read(current);
                    current += ReaderCount::size();
                }
                count--;
                currentValue2 = Reader2::read(current);
                current += Reader2::size();
                v1 = key;
                v2 = currentValue1;
                v3 = currentValue2;
            }
            return current < end;
        }

        bool hasNext() {
            return current < end;
        }

        void setup(const char* start, const char *end) {
	    initializeConstraints();
            this->start = start;
            current = start;
            this->end = end;
            assert(current <= end);
            currentValue1 = currentValue2 = -1;
            isSecondColumnIgnored = false;
            count = countgroup = 0;
        }

        void setup(long c1, const char* start, const char *end) {
            //Get the right range
            const char *s = start;
            const char *e = end;
            while (s != e) {
                long v1 = Reader1::read(s);
                long c = ReaderCount::read(s + Reader1::size());
                if (v1 == c1) {
                    e = s + Reader1::size() + ReaderCount::size() + c * Reader2::size();
                    break;
                } else if (v1 > c1) {
                    s = e = end;
                    break;
                }
                s += Reader1::size() + ReaderCount::size() + c * Reader2::size();
            }
            if (e < s)
                e = s;
            setup(s, e);
        }

        void setup(long c1, long c2, const char* start, const char *end) {
            //Get the right range
            const char *s = start;
            const char *e = end;
            while (s != e) {
                long v1 = Reader1::read(s);
                long c = ReaderCount::read(s + Reader1::size());
                if (v1 == c1) {
                    e = s + Reader1::size() + ReaderCount::size() + c * Reader2::size();
                    break;
                } else if (v1 > c1) {
                    s = e = end;
                    break;
                }
                s += Reader1::size() + ReaderCount::size() + c * Reader2::size();
            }
            if (e < s) {
                e = s;
            }
            setup(s, e);
            if (s < e) {
                current += Reader1::size();
                currentValue1 = c1;
                count = countgroup = ReaderCount::read(current);
                current += ReaderCount::size();
                s += Reader1::size() + ReaderCount::size();
                //Do binary search
                while (s < e) {
                    const size_t diff = e - s;
                    const size_t middlevalue = (diff / Reader2::size()) >> 1;
                    const char *m = s + middlevalue * Reader2::size();
                    long vm = Reader2::read(m);
                    if (vm < c2) {
                        s = m + Reader2::size();
                    } else if (vm > c2) {
                        e = m;
                    } else {
                        current = m;
                        break;
                    }
                }
                long v2 = Reader2::read(current);
                if (v2 == c2) {
                    this->end = current + Reader2::size();
                    count = countgroup = 1;
                } else {
                    current = end;
                }
            }

            /*setup(start, end);
              if (hasNext()) {
              next();
              moveto(c1, c2);
              if (!hasNext()) {
              next();
              if (getValue1() != c1 || getValue2() != c2) {
              current = end;
              }
            //reset the iterator
            } else {
            current = end;
            }
            }*/
        }

        void first() {
            next();
        }

        void moveto(const long c1, const long c2) {
            if (!hasNext() && (c1 > currentValue1 || (!isSecondColumnIgnored && currentValue1 == c1
                            && c2 > currentValue2))) {
                return;
            }

            long currentterm = currentValue1;
            if (c1 > currentValue1) {
                //Move to the end of the current group
                if (count != 0 && !isSecondColumnIgnored) {
                    current += Reader2::size() * count;
                }

                // Read the first entry
                while (current != end) {
                    currentterm = Reader1::read(current);
                    if (currentterm >= c1) {
                        break;
                    }
                    current += Reader1::size();
                    const long c = ReaderCount::read(current);
                    current += ReaderCount::size() + c * Reader2::size();
                }
                count = countgroup = 0;
                currentValue2 = -1;
            }

            if (isSecondColumnIgnored) {
                //Re-read the first entry
                if (c1 <= currentValue1) {
                    current -= Reader1::size() + ReaderCount::size() + countgroup * Reader2::size();
                }
            } else {
                //Second term
                if (currentterm == c1) {
                    if (countgroup == 0)
                        next();
                    if (c2 <= currentValue2) {
                        count++;
                        current -= Reader2::size();
                    } else {
                        const char *e = current + count * Reader2::size();
                        const char *orige = e;
                        while (current < e) {
                            const size_t diff = e - current;
                            const size_t middlevalue = (diff / Reader2::size()) >> 1;
                            const char *m = current + middlevalue * Reader2::size();
                            long vm = Reader2::read(m);
                            if (vm < c2) {
                                current = m + Reader2::size();
                            } else if (vm > c2) {
                                e = m;
                            } else {
                                current = m;
                                break;
                            }
                        }
                        //update counter
                        count = (orige - current) / Reader2::size();
                    }
                } else {
                    if (currentValue2 == -1) {
                        //Nothing to do
                    } else {
                        count++;
                        current -= Reader2::size();
                    }
                }
            }
        }

        void mark() {
            m_current = current;
            m_currentValue1 = currentValue1;
            m_currentValue2 = currentValue2;
            m_count = count;
            m_countgroup = countgroup;
        }

        void reset(const char i) {
            current = m_current;
            currentValue1 = m_currentValue1;
            currentValue2 = m_currentValue2;
            count = m_count;
            countgroup = m_countgroup;
        }

        void ignoreSecondColumn() {
            isSecondColumnIgnored = true;
            if (currentValue1 != -1) {
                //Jump to the next value
                current += Reader2::size() * count;
                count = 0;
            }
        }

        long getCount() {
            if (isSecondColumnIgnored) {
                return countgroup;
            } else {
                return 1;
            }
        }

        int getTypeItr() {
            return NEWCLUSTER_ITR;
        }


        static long s_getValue1AtRow(const char *start, const long rowId) {
            const char *pos = start + (Reader1::size() +
                    Reader2::size() +
                    ReaderCount::size()) * rowId;
            return Reader1::read(pos);
        }
};

#endif

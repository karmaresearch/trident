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


#ifndef _NEWCOLUMNTABLE_H
#define _NEWCOLUMNTABLE_H

//#include <trident/iterators/pairitr.h>
#include <trident/binarytables/newtable.h>
#include <trident/kb/consts.h>
#include <kognac/utils.h>

#include <assert.h>

class SequenceWriter {
    public:
        virtual void add(const uint64_t v) {
            throw 10; //The default class should never be used
        }
};

class NewColumnTable: public AbsNewTable {
    private:
        uint8_t bytesPerFirstEntry, bytesPerSecondEntry;
        uint8_t bytesPerCount, bytesPerStartingPoint;
        uint8_t bytesFirstBlock;

        int64_t currentValue1, currentValue2;
        uint64_t nTerms, nUniqueFirstTerms;

        uint64_t currentCount, scannedCounts;

        const char *start;
        const char *end;
        const char *startpos1;
        const char *startpos2;
        const char *startblock2;
        const char *currentpos1;
        const char *currentpos2;
        bool isSecondColumnIgnored;
        bool limitsSet;
#if DEBUG
        bool movetoAllowed;
        bool hasNextCalled;
        bool hasNextRetval;
#endif

        // For mark/reset
        int64_t savedCurrentValue1;
        const char *savedCurrentpos1;
        uint64_t savedCurrentCount;
        uint64_t savedScannedCounts;
        const char *savedStartblock2;
        int64_t savedCurrentValue2;
        const char *savedCurrentpos2;
#if DEBUG
        bool savedmovetoAllowed;
        bool savedhasNextCalled;
        bool savedhasNextRetval;
#endif

        static void columnNotIn11(const char *begin1, const char* end1,
                const uint8_t bEntry1, const uint8_t bBlock1,
                const char *begin2, const char *end2,
                const uint8_t bEntry2, const uint8_t bBlock2,
                SequenceWriter *output);

        static void columnNotIn12(const char *begin1, const char* end1,
                const uint8_t bEntry1, const uint8_t bBlock1,
                const char *begin2, const char *end2,
                const uint8_t bEntry2,
                SequenceWriter *output);

        static void columnNotIn21(const char *begin1, const char* end1,
                const uint8_t bEntry1,
                const char *begin2, const char *end2,
                const uint8_t bEntry2, const uint8_t bBlock2,
                SequenceWriter *output);

        static void columnNotIn22(const char *begin1, const char* end1,
                const uint8_t bEntry1,
                const char *begin2, const char *end2,
                const uint8_t bEntry2,
                SequenceWriter *output);

    public:

        char getReaderSize1() const {
            return bytesPerFirstEntry;
        }

        char getReaderSize2() const {
            return bytesPerSecondEntry;
        }

        char getReaderCountSize() const {
            return bytesPerCount;
        }

        char getReaderStartingPointSize() const {
            return bytesPerStartingPoint;
        }

        int64_t getValue1() {
            return currentValue1;
        }

        int64_t getValue2() {
            return currentValue2;
        }

        uint64_t getCardinality() {
            if (isSecondColumnIgnored) {
                return nUniqueFirstTerms;
            } else {
                return nTerms;
            }
        }

        const char *getUnderlyingArray(uint8_t pos) {
            if (limitsSet) {
                return NULL;
            }
            if (pos == 1) {
                return startpos1;
            } else {
                return startpos2;
            }
        }

        uint64_t estCardinality() {
            return getCardinality();
        }


        bool hasNext() {
            bool retval = currentpos1 < startpos2 ||
                (!isSecondColumnIgnored && currentpos2 < end);
#if DEBUG
            hasNextCalled = true;
            hasNextRetval = retval;
#endif
            return retval;
        }

        void next() {
            //Read first term
            assert(hasNext());
#if DEBUG
            movetoAllowed = true;
#endif
            if (isSecondColumnIgnored || scannedCounts == currentCount) {
                currentValue1 = Utils::decode_longFixedBytes(currentpos1, bytesPerFirstEntry);
                currentpos1 += bytesPerFirstEntry;
                currentCount = Utils::decode_longFixedBytes(currentpos1, bytesPerCount);
                currentpos1 += bytesPerCount + bytesPerStartingPoint;
                scannedCounts = 0;
                startblock2 = currentpos2;
            }

            if (!isSecondColumnIgnored) {
                //Read second term
                currentValue2 = Utils::decode_longFixedBytes(currentpos2, bytesPerSecondEntry);
                currentpos2 += bytesPerSecondEntry;
                scannedCounts++;
            }
        }

        bool next(int64_t &v1, int64_t &v2, int64_t &v3) {
            next();
            v2 = currentValue1;
            v3 = currentValue2;
            return hasNext();
        }

        void first() {
            next();
        }

        void moveto(const int64_t c1, const int64_t c2) {
#if DEBUG
            assert(bytesFirstBlock > 0);
            assert(bytesPerSecondEntry > 0);
            assert(currentValue1 != -1);
            assert(hasNextCalled);
            if (! hasNextRetval) {
                return;
            }
            // assert(hasNextRetval);
            assert(movetoAllowed);
            assert(currentValue2 != -1 || isSecondColumnIgnored);

            movetoAllowed = false;
            hasNextCalled = false;
#endif
            if (!hasNext() && (c1 > currentValue1 ||
                        (!isSecondColumnIgnored && c1 == currentValue1 && c2 > currentValue2))) {
                return;
            }

            bool searchsecondterm = c1 == currentValue1;
            if (c1 > currentValue1) {
                //Binary search
                const char *s = currentpos1;
                const char *e = startpos2;
                bool found = false;
                uint64_t middleValue;
                while (s < e) {
                    const uint64_t middlePos = (e - s) / bytesFirstBlock / 2;
                    const char *middle = s + middlePos * bytesFirstBlock;
                    middleValue  = Utils::decode_longFixedBytes(middle, bytesPerFirstEntry);
                    if (middleValue < c1) {
                        s = middle + bytesFirstBlock;
                    } else if (middleValue > c1) {
                        e = middle;
                    } else { //Found
                        currentpos1 = middle;
                        const uint64_t idx2 = Utils::decode_longFixedBytes(
                                middle + bytesPerFirstEntry + bytesPerCount,
                                bytesPerStartingPoint);
                        currentpos2 = startpos2 + idx2 * bytesPerSecondEntry;
                        startblock2 = currentpos2;
                        currentCount = 0;
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    if (s >= startpos2) {
                        //No more entries
                        currentpos1 = startpos2;
                        currentpos2 = end;
                    } else {
                        currentpos1 = s;
                        currentCount = 0;
                        const uint64_t pos2 = Utils::decode_longFixedBytes(
                                s + bytesPerFirstEntry + bytesPerCount,
                                bytesPerStartingPoint);
                        currentpos2 = startpos2 + pos2 * bytesPerSecondEntry;
                        startblock2 = currentpos2;
                    }
                } else {
                    searchsecondterm = true;
                }
                scannedCounts = 0;
                currentValue2 = -1;
            } else if (isSecondColumnIgnored) {
                //Re-read the current entry
                currentpos1 -= bytesPerFirstEntry + bytesPerCount + bytesPerStartingPoint;
            }

            if (!isSecondColumnIgnored) {
                if (searchsecondterm && c2 > currentValue2) {
                    if (currentValue2 == -1) {
                        //In this case, I must read all the first term already,
                        //otherwise the following next() will screw it up
                        currentValue1 = Utils::decode_longFixedBytes(currentpos1, bytesPerFirstEntry);
                        currentpos1 += bytesPerFirstEntry;
                        currentCount = Utils::decode_longFixedBytes(currentpos1, bytesPerCount);
                        currentpos1 += bytesPerCount + bytesPerStartingPoint;
                    }

                    const char *s = currentpos2;
                    const char *e = startblock2 + currentCount * bytesPerSecondEntry;
                    bool found = false;
                    while (s < e) {
                        const uint64_t middlePos = (e - s) / bytesPerSecondEntry / 2;
                        const char *middle = s + middlePos * bytesPerSecondEntry;
                        const uint64_t middleValue = Utils::decode_longFixedBytes(
                                middle,
                                bytesPerSecondEntry);
                        if (middleValue < c2) {
                            s = middle + bytesPerSecondEntry;
                        } else if (middleValue > c2) {
                            e = middle;
                        } else { //Found
                            found = true;
                            scannedCounts = (middle - startblock2) / bytesPerSecondEntry;
                            currentpos2 = middle;
                            break;
                        }
                    }
                    if (!found) {
                        if (s >= startblock2 + currentCount * bytesPerSecondEntry) {
                            scannedCounts = currentCount;
                            currentpos2 = startblock2 + currentCount * bytesPerSecondEntry;
                        } else {
                            currentpos2 = s;
                            scannedCounts = (s - startblock2) / bytesPerSecondEntry;
                        }
                        if (currentpos2 == end) {
                            currentValue2 = 0;
                        }
                    }
                } else if (currentValue2 != -1) {
                    //I do a step back so that hasNext() and next() will point to the same value
                    scannedCounts--;
                    currentpos2 -= bytesPerSecondEntry;
                    return;
                }
            }
        }

        void clear() {
        }

        void mark() {
            savedCurrentValue1 = currentValue1;
            savedCurrentpos1 = currentpos1;
            savedCurrentCount = currentCount;
            savedScannedCounts = scannedCounts;
            savedStartblock2 = startblock2;
            savedCurrentValue2 = currentValue2;
            savedCurrentpos2 = currentpos2;
#if DEBUG
            savedmovetoAllowed = movetoAllowed;
            savedhasNextCalled = hasNextCalled;
            savedhasNextRetval = hasNextRetval;
#endif
        }

        void reset(const char i) {
            currentValue1 = savedCurrentValue1;
            currentpos1 = savedCurrentpos1;
            currentCount = savedCurrentCount;
            scannedCounts = savedScannedCounts;
            startblock2 = savedStartblock2;
            currentValue2 = savedCurrentValue2;
            currentpos2 = savedCurrentpos2;
#if DEBUG
            movetoAllowed = savedmovetoAllowed;
            hasNextCalled = savedhasNextCalled;
            hasNextRetval = savedhasNextRetval;
#endif
        }

        int64_t getCount() {
            if (isSecondColumnIgnored)
                return currentCount;
            else
                return 1;
        }

        void ignoreSecondColumn() {
            isSecondColumnIgnored = true;
            if (currentValue1 != -1) {
                //I must move to the next first element, if any
                scannedCounts = currentCount;
            }
        }

        int getTypeItr() {
            return NEWCOLUMN_ITR;
        }

        void columnNotIn(uint8_t columnId, NewColumnTable *other,
                uint8_t columnOther, SequenceWriter *output);

        void setup(const char* start, const char *end) {
            initializeConstraints();
            this->start = start;
            this->end = end;
            currentValue1 = currentValue2 = -1;
            isSecondColumnIgnored = false;
            limitsSet = false;

            //get all column widths
            uint8_t header1 = (uint8_t) start[0];
            bytesPerFirstEntry = (header1 >> 3) & 7;
            bytesPerSecondEntry = (header1) & 7;
            uint8_t header2 = (uint8_t) start[1];
            bytesPerStartingPoint =  header2 & 7;
            bytesPerCount = (header2 >> 3) & 7;
            bytesFirstBlock = bytesPerFirstEntry + bytesPerCount + bytesPerStartingPoint;

            int offset = 2;
            nUniqueFirstTerms = Utils::decode_vlong2(start, &offset);
            nTerms = Utils::decode_vlong2(start, &offset);

            currentpos1 = startpos1 = start + offset;
            currentpos2 = startpos2 = startpos1 + (bytesFirstBlock) * nUniqueFirstTerms;
            scannedCounts = currentCount = 0;

            assert(bytesPerFirstEntry > 0);
            assert(bytesPerSecondEntry > 0);
        }

        void setup(int64_t c1, const char* s, const char *e) {
            //Search for the right c1. Then sets the limits.
            setup(s, e);
            limitsSet = true;
            if (hasNext()) {
                next();
                moveto(c1, 0);
                if (hasNext()) {
                    next();
                    if (getValue1() == c1) {
                        //Sets new limits
                        end = startblock2 + currentCount * bytesPerSecondEntry;
                        currentpos2 = startblock2;
                        startpos2 = currentpos1;
                        startpos1 = startpos2 - bytesFirstBlock;
                        currentpos1 = startpos1;
                        nUniqueFirstTerms = 1;
                        nTerms = currentCount;
                    } else {
                        //Make it fail
                        currentpos1 = startpos2;
                        currentpos2 = end;
                    }
                } else {
                    //Make it fail
                    currentpos1 = startpos2;
                    currentpos2 = end;
                }
            }
        }

        void setup(int64_t c1, int64_t c2, const char* s, const char *e) {
            //Search for the right c1. Then sets the limits.
            setup(s, e);
            limitsSet = true;
            if (hasNext()) {
                next();
                moveto(c1, c2);
                if (hasNext()) {
                    next();
                    currentpos1 = startpos2;
                    if (getValue1() == c1 && getValue2() == c2) {
                        nUniqueFirstTerms = 1;
                        nTerms = 1;
                        end = currentpos2;
                        currentpos2 -= bytesPerSecondEntry;
                        scannedCounts--;
                    } else {
                        //Make it fail
                        currentpos2 = end;
                    }
                } else {
                    //Make it fail
                    currentpos1 = startpos2;
                    currentpos2 = end;
                }
            }
        }

        int64_t getValue1AtRow(int64_t rowid) {
            const char *pos = startpos1 + bytesFirstBlock * rowid;
            return Utils::decode_longFixedBytes(pos, bytesPerFirstEntry);
        }

        int64_t getValue2AtRow(int64_t rowid) {
            const char *pos = startblock2 + bytesPerSecondEntry * rowid;
            return Utils::decode_longFixedBytes(pos, bytesPerSecondEntry);
        }

        template<int nbytes, int nskip>
            static int64_t s_getValue1AtRow(const char *start,
                    const int64_t rowId) {
                const char *currentpos1= start + (nbytes + nskip) * rowId;
                const int64_t v = Utils::decode_longFixedBytes(currentpos1, nbytes);
                return v;
            }

        template<int nbytes1, int nbytes2>
            static void s_getValue12AtRow(const uint64_t sizetable,
                    const uint8_t offset,
                    const char *start,
                    const uint64_t rowId,
                    uint64_t &v1,
                    uint64_t &v2) {
                const char *currentpos1 = start + (nbytes1 + offset) * rowId;
                v1 = Utils::decode_longFixedBytes(currentpos1, nbytes1);
                const char *currentpos2 = start + (nbytes1 + offset) * sizetable +
                    rowId * nbytes2;
                v2 = Utils::decode_longFixedBytes(currentpos2, nbytes2);
            }

};

#endif

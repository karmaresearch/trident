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


#include <trident/binarytables/newcolumntableinserter.h>
#include <kognac/utils.h>

void NewColumnTableInserter::startAppend() {
    tmpfirstpairs.clear();
    tmpsecondpairs.clear();
    largestGroup = largestElement1 = largestElement2 = 0;

    prevtotalsize2 = 0;
    prevel1 = -1;
    offloadedElements1 = 0;
    offloadedElements2 = 0;
    fileopen1 = false;
    fileopen2 = false;
}

void NewColumnTableInserter::append(int64_t t1, int64_t t2) {
    if (prevel1 != t1) {
        const int64_t totalsize2 = tmpsecondpairs.size() + offloadedElements2;
        if (totalsize2 - prevtotalsize2 > largestGroup) {
            largestGroup = totalsize2 - prevtotalsize2;
        }
        tmpfirstpairs.push_back(std::make_pair(t1, totalsize2));
        prevtotalsize2 = totalsize2;
        prevel1 = t1;
    }
    tmpsecondpairs.push_back(t2);
    if (t2 >= largestElement2) {
        largestElement2 = t2;
    }
    if (t1 >= largestElement1) {
        largestElement1 = t1;
    }

    //Offload all first terms to disk if too many
    if (tmpfirstpairs.size() >= thresholdToOffload) {
        if (!fileopen1) {
            offloadfile1.open(getRootDir() + "/tmpfile1" + to_string(perm));
            fileopen1 = true;
        }
        char buffer[16 * 1000];
        int sizebuffer = 0;
        for (size_t i = 0; i < tmpfirstpairs.size(); ++i) {
            if (sizebuffer == 16 * 1000) {
                offloadfile1.write(buffer, 16 * 1000);
                sizebuffer = 0;
            }
            Utils::encode_long(buffer + sizebuffer, tmpfirstpairs[i].first);
            Utils::encode_long(buffer + sizebuffer + 8, tmpfirstpairs[i].second);
            sizebuffer += 16;
            offloadedElements1++;
        }
        if (sizebuffer > 0) {
            offloadfile1.write(buffer, sizebuffer);
            if (offloadfile1.fail()) {
                LOG(ERRORL) << "Failed in writing offloadfile1";
                throw 10;
            }
        }
        tmpfirstpairs.clear();
    }

    //Offload all second terms to disk if too many
    if (tmpsecondpairs.size() >= thresholdToOffload) {
        if (!fileopen2) {
            //Open a tmp file
            offloadfile2.open(getRootDir() + "/tmpfile2" + to_string(perm));
            fileopen2 = true;
        }
        char buffer[8 * 1000];
        int sizebuffer = 0;
        for (size_t i = 0; i < tmpsecondpairs.size(); ++i) {
            if (sizebuffer == 8 * 1000) {
                offloadfile2.write(buffer, 8 * 1000);
                sizebuffer = 0;
            }
            Utils::encode_long(buffer + sizebuffer, tmpsecondpairs[i]);
            sizebuffer += 8;
            offloadedElements2++;
        }
        if (sizebuffer > 0) {
            offloadfile2.write(buffer, sizebuffer);
            if (offloadfile2.fail()) {
                LOG(ERRORL) << "Failed in writing offloadfile2";
                throw 10;
            }
        }
        tmpsecondpairs.clear();
    }
}

void NewColumnTableInserter::stopAppend() {
    //Check largest group
    const int64_t totalsize2 = tmpsecondpairs.size() + offloadedElements2;
    if (totalsize2 - prevtotalsize2 > largestGroup) {
        largestGroup = totalsize2 - prevtotalsize2;
    }

    //First determine the size of the maximum element in both columns
    uint8_t bytesPerFirstEntry = Utils::numBytesFixedLength(largestElement1);
    uint8_t bytesPerSecondEntry = Utils::numBytesFixedLength(largestElement2);
    uint8_t bytesPerCount = Utils::numBytesFixedLength(largestGroup);
    uint8_t bytesPerOffset = Utils::numBytesFixedLength(totalsize2);
    if (bytesPerFirstEntry == 0 || bytesPerSecondEntry == 0 ||
            bytesPerCount == 0 || bytesPerOffset == 0) {
        LOG(ERRORL) << "Bytes are incorrect";
        throw 10;
    }

    //Write the header
    uint8_t header1 = (bytesPerFirstEntry << 3) + (bytesPerSecondEntry & 7);
    writeByte(header1);
    uint8_t header2 = (bytesPerCount << 3) + (bytesPerOffset & 7);
    writeByte(header2);
    writeVLong2(tmpfirstpairs.size() + offloadedElements1);
    writeVLong2(totalsize2);

    //write all first elements that were offloaded
    if (offloadedElements1 > 0) {
        assert(fileopen1);
        offloadfile1.close();
        offloadfile1_r.open(getRootDir() + "/tmpfile1" + to_string(perm));
        char *buffer = new char[16 * 1000];

        int64_t prevkey = -1;
        int64_t prevsize = -1;
        while (offloadedElements1 > 0) {
            const int64_t maxElsToRead = min(offloadedElements1, (int64_t)1000);
            offloadfile1_r.read(buffer, maxElsToRead * 16);
            for (int64_t i = 0; i < maxElsToRead * 16; i += 16) {
                const int64_t v1 = Utils::decode_long(buffer + i);
                const int64_t v2 = Utils::decode_long(buffer + i + 8);
                if (prevkey != -1) {
                    writeLong(bytesPerFirstEntry, prevkey);
                    writeLong(bytesPerCount, v2 - prevsize);
                    writeLong(bytesPerOffset, prevsize);
                }
                prevkey = v1;
                prevsize = v2;
            }
            offloadedElements1 -= maxElsToRead;
        }
        //Must write the last element
        assert(prevkey != -1);
        if (tmpfirstpairs.size() > 0) {
            writeLong(bytesPerFirstEntry, prevkey);
            writeLong(bytesPerCount, tmpfirstpairs.front().second - prevsize);
            writeLong(bytesPerOffset, prevsize);
        } else {
            writeLong(bytesPerFirstEntry, prevkey);
            writeLong(bytesPerCount, totalsize2 - prevsize);
            writeLong(bytesPerOffset, prevsize);
        }

        delete[] buffer;
        assert(offloadedElements1 == 0);
        offloadfile1_r.close();
        Utils::remove(getRootDir() + "/tmpfile1" + to_string(perm));
    }

    //Write all first elements
    for (size_t i = 0; i < tmpfirstpairs.size(); ++i) {
        std::pair<uint64_t, uint64_t> v = tmpfirstpairs[i];
        writeLong(bytesPerFirstEntry, v.first);
        if (i < tmpfirstpairs.size() - 1) {
            writeLong(bytesPerCount, tmpfirstpairs[i + 1].second - v.second);
        } else {
            writeLong(bytesPerCount, totalsize2 - v.second);
        }
        writeLong(bytesPerOffset, v.second);
    }

    //Write all second elements that were offloaded
    if (offloadedElements2 > 0) {
        assert(fileopen2);
        offloadfile2.close();
        offloadfile2_r.open(getRootDir() + "/tmpfile2" + to_string(perm), ios_base::in);
        char buffer[8 * 1000];
        while (offloadedElements2 > 0) {
            const int64_t maxElsToRead = min(offloadedElements2, (int64_t)1000);
            offloadfile2_r.read(buffer, maxElsToRead * 8);
            for (int64_t i = 0; i < maxElsToRead * 8; i += 8) {
                const int64_t value = Utils::decode_long(buffer + i);
                writeLong(bytesPerSecondEntry, value);
            }
            offloadedElements2 -= maxElsToRead;
        }
        assert(offloadedElements2 == 0);
        offloadfile2_r.close();
        Utils::remove(getRootDir() + "/tmpfile2" + to_string(perm));
    }

    //Write all second elements
    for (const auto v : tmpsecondpairs) {
        writeLong(bytesPerSecondEntry, v);
    }
}

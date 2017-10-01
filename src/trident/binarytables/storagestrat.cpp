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


#include <trident/binarytables/storagestrat.h>

/*unsigned StorageStrat::getStrat1() {
  unsigned output = 0;
  output = StorageStrat::setStorageType(output, CLUSTER_ITR);
  output = StorageStrat::setCompr1(output, COMPR_2);
  output = StorageStrat::setCompr2(output, COMPR_2);
  output = StorageStrat::setDiff1(output, NO_DIFFERENCE);
  return output;
  }

  unsigned StorageStrat::getStrat2() {
  unsigned output = 0;
  output = StorageStrat::setStorageType(output, ROW_ITR);
  output = StorageStrat::setCompr1(output, COMPR_2);
  output = StorageStrat::setCompr2(output, COMPR_2);
  output = StorageStrat::setDiff1(output, NO_DIFFERENCE);
  return output;
  }

  unsigned StorageStrat::getStrat3() {
  unsigned output = 0;
  output = StorageStrat::setStorageType(output, COLUMN_ITR);
  output = StorageStrat::setCompr1(output, COMPR_2);
  output = StorageStrat::setCompr2(output, COMPR_2);
  output = StorageStrat::setDiff1(output, NO_DIFFERENCE);
  return output;
  }

  unsigned StorageStrat::getStrat4() {
  unsigned output = 0;
  output = StorageStrat::setStorageType(output, ROW_ITR);
  output = StorageStrat::setCompr1(output, NO_COMPR);
  output = StorageStrat::setCompr2(output, NO_COMPR);
  output = StorageStrat::setDiff1(output, NO_DIFFERENCE);
  return output;
  }*/

unsigned StorageStrat::getStrat5() {
    unsigned output = 0;
    output = StorageStrat::setStorageType(output, NEWCOLUMN_ITR);
    return output;
}

unsigned StorageStrat::getStrat6() {
    unsigned output = 0;
    output = StorageStrat::setStorageType(output, NEWROW_ITR);
    output = StorageStrat::setBytesField1(output, 3); //5 bytes
    output = StorageStrat::setBytesField2(output, 3); //5 bytes
    return output;
}
unsigned StorageStrat::getStrat7() {
    unsigned output = 0;
    output = StorageStrat::setStorageType(output, NEWCLUSTER_ITR);
    output = StorageStrat::setBytesField1(output, 3); //5 bytes
    output = StorageStrat::setBytesField2(output, 3); //5 bytes
    output = output | 1; //set counter to four bytes
    return output;
}


const unsigned StorageStrat::FIXEDSTRAT5 = getStrat5();
const unsigned StorageStrat::FIXEDSTRAT6 = getStrat6();
const unsigned StorageStrat::FIXEDSTRAT7 = getStrat7();

PairItr *StorageStrat::getBinaryTable(const char signature) {
    int storageType = getStorageType(signature);
    if (storageType == NEWCOLUMN_ITR) {
          NewColumnTable *ph = f4->get();
          return ph;
      } else if (storageType == NEWROW_ITR) {
          const char nbytes1 = (signature >> 3) & 3;
          const char nbytes2 = (signature >> 1) & 3;
          return f5->get(nbytes1, nbytes2);
      } else if (storageType == NEWCLUSTER_ITR) {
          const char nbytes1 = (signature >> 3) & 3;
          const char nbytes2 = (signature >> 1) & 3;
          char ncount = 1;
          if (signature & 1)
              ncount = 4;
          return f6->get(nbytes1, nbytes2, ncount);
      } else {
          throw 10;
      }
}

BinaryTableInserter *StorageStrat::getBinaryTableInserter(const char signature) {
    int storageType = getStorageType(signature);
    if (storageType == ROW_ITR) {
        statsRow++;
        RowTableInserter *ph = f1i->get();
        ph->setCompressionMode(getCompr1(signature), getCompr2(signature));
        ph->setDifferenceMode(getDiff1(signature));
        return ph;
    } else if (storageType == CLUSTER_ITR)  {
        statsCluster++;
        ClusterTableInserter *ph = f2i->get();
        ph->mode_compression(getCompr1(signature), getCompr2(signature));
        ph->mode_difference(getDiff1(signature));
        return ph;
    } else if (storageType == COLUMN_ITR) {
        statsColumn++;
        ColumnTableInserter *ph = f3i->get();
        ph->setCompressionMode(getCompr1(signature), getCompr2(signature));
        return ph;
    } else if (storageType == NEWCOLUMN_ITR) {
        NewColumnTableInserter *ph = f4i->get();
        return ph;
    } else if (storageType == NEWROW_ITR) {
        NewRowTableInserter *ph = f5i->get();
        const char nbytes1 = (signature >> 3) & 3;
        const char nbytes2 = (signature >> 1) & 3;
        ph->setReaderSizes(nbytes1, nbytes2);
        return ph;
    } else if (storageType == NEWCLUSTER_ITR) {
        NewClusterTableInserter *ph = f6i->get();
        const char nbytes1 = (signature >> 3) & 3;
        const char nbytes2 = (signature >> 1) & 3;
        char ncount;
        if (signature & 1) {
            ncount = 4;
        } else {
            ncount = 1;
        }
        ph->setSizes(nbytes1, nbytes2, ncount);
        return ph;
    } else {
        throw 10;
    }
}

char StorageStrat::getStrategy(int typeStorage, int diff, int compr1,
        int compr2, bool aggre) {
    unsigned strat = 0;
    strat = setStorageType(strat, typeStorage);
    if (typeStorage == COLUMN_ITR || typeStorage == ROW_ITR ||
            typeStorage == CLUSTER_ITR) {
        strat = setDiff1(strat, diff);
        strat = setCompr1(strat, compr1);
        strat = setCompr2(strat, compr2);
    } else {
        strat = setBytesField1(strat, compr1);
        strat = setBytesField2(strat, compr1);
    }
    if (aggre) {
        assert(typeStorage == ROW_ITR || typeStorage == NEWROW_ITR);
        strat = setAggregated(strat, aggre);
    }
    return (char) strat;
}

long StorageStrat::minsum(const long counters1[2][2], const long counters2[2], int &oc1, int &oc2, int &od) {
    long minSum = LONG_MAX;
    for (int c1 = 0; c1 < 2; c1++) {
        for (int delta = 0; delta < 2; ++delta) {
            for (int c2 = 0; c2 < 2; c2++) {
                long sum = counters1[c1][delta] + counters2[c2];
                if (sum < minSum) {
                    minSum = sum;
                    oc1 = c1;
                    oc2 = c2;
                    od = delta;
                }
            }
        }
    }
    return minSum;
}

size_t StorageStrat::getBinaryBreakingPoint() {
    //Used only to ensure the compiler is not optimizing the code out
    long fakeSum = 0;

    //Anything below 8 can be search faster with a linear search
    int i = 8;
    for (; i < 4096; (i > 32) ? i += 32 : i *= 2) {
        std::vector<long> vector1;

        //Fill the table
        for (int j = 0; j < i; ++j) {
            vector1.push_back(rand());
        }

        //Sort the array
        std::sort(vector1.begin(), vector1.end());

        std::vector<size_t> idsToSearch;
        for (int j = 0; j < 1000; ++j) {
            idsToSearch.push_back(rand() % i);
        }

        std::chrono::duration<double> linearSec[31];
        for (int m = 0; m < 31; ++m) {
            //Search linearly
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            for (std::vector<size_t>::const_iterator itr = idsToSearch.begin();
                    itr != idsToSearch.end(); ++itr) {
                std::vector<long>::const_iterator itrP = vector1.begin();
                while (itrP != vector1.end()) {
                    if (*itrP >= vector1[*itr]) {
                        break;
                    }
                    itrP++;
                }
                fakeSum += m + ((itrP != vector1.end() && *itrP == vector1[*itr]) ? 1 : 0);
            }
            linearSec[m] = std::chrono::system_clock::now() - start;
        }

        //Search with binary search
        std::chrono::duration<double> binarySec[31];
        for (int m = 0; m < 31; ++m) {
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            for (std::vector<size_t>::const_iterator itr = idsToSearch.begin();
                    itr != idsToSearch.end(); ++itr) {
                fakeSum += m + ((std::binary_search(vector1.begin(), vector1.end(),
                                vector1[*itr])) ? 1 : 0);
            }
            binarySec[m] = std::chrono::system_clock::now() - start;
        }

        int linearWins = 0;
        for (int m = 0; m < 31; ++m) {
            if (linearSec[m].count() <= binarySec[m].count())
                linearWins++;
        }

        if (linearWins < 16) {
            return i > 32 ? i - 32 : i / 2;
        }

        //This is a simple check to ensure the compiler is not deleting the sum
        if (fakeSum == 0)
            throw 10;
    }
    return 4096;
}

bool StorageStrat::determineAggregatedStrategy(long *v1, long *v2, const int size,
        const long nTerms, Statistics &stats) {
    //return true if v1 contains many duplicates

    //For every different v1, there are at least two different pairs, one with the value and one with the coordinates.
    long unique = 1;
    for (size_t i = 1; i < size; ++i) {
        if (v1[i - 1] != v1[i]) {
            unique++;
        }
    }
    if (unique <= size / 10) {
        stats.aggregated++;
        return true; //aggregate
    } else {
        stats.notAggregated++;
        return false; //do not aggregate
    }
}

void StorageStrat::createAllCombinations(std::vector<Combinations> &output,
        long *groupCounters1Compr2, long *listCounters1Compr2,
        long groupCounters2Compr2, long listCounters2Compr2,
        long *groupCounters1Compr1, long *listCounters1Compr1,
        long groupCounters2Compr1, long listCounters2Compr1) {

    for (int diff = 0; diff < 2; ++diff) {
        //group
        Combinations c;
        c.diffMode = diff;
        c.type = CLUSTER_ITR;
        c.compr1Mode = COMPR_2;
        c.compr2Mode = COMPR_2;
        c.sum = groupCounters1Compr2[diff] + groupCounters2Compr2;
        output.push_back(c);
        c.compr1Mode = COMPR_2;
        c.compr2Mode = COMPR_1;
        c.sum = groupCounters1Compr2[diff] + groupCounters2Compr1;
        output.push_back(c);
        c.compr1Mode = COMPR_1;
        c.compr2Mode = COMPR_2;
        c.sum = groupCounters1Compr1[diff] + groupCounters2Compr2;
        output.push_back(c);
        c.compr1Mode = COMPR_1;
        c.compr2Mode = COMPR_1;
        c.sum = groupCounters1Compr1[diff] + groupCounters2Compr1;
        output.push_back(c);

        //row
        c.type = ROW_ITR;
        c.compr1Mode = COMPR_2;
        c.compr2Mode = COMPR_2;
        c.sum = listCounters1Compr2[diff] + listCounters2Compr2;
        output.push_back(c);
        c.compr1Mode = COMPR_2;
        c.compr2Mode = COMPR_1;
        c.sum = listCounters1Compr2[diff] + listCounters2Compr1;
        output.push_back(c);
        c.compr1Mode = COMPR_1;
        c.compr2Mode = COMPR_2;
        c.sum = listCounters1Compr1[diff] + listCounters2Compr2;
        output.push_back(c);
        c.compr1Mode = COMPR_1;
        c.compr2Mode = COMPR_1;
        c.sum = listCounters1Compr1[diff] + listCounters2Compr1;
        output.push_back(c);
    }
}

bool combinationSorter(const StorageStrat::Combinations &c1, const StorageStrat::Combinations &c2) {
    if (c1.sum == c2.sum) {
        if (c1.diffMode == NO_DIFFERENCE && c2.diffMode == DIFFERENCE) {
            return true;
        } else if (c1.diffMode == DIFFERENCE && c2.diffMode == NO_DIFFERENCE) {
            return false;
        }

        //compr2 goes first
        if (c1.compr1Mode == COMPR_2 && c2.compr1Mode == COMPR_1) {
            return true;
        } else if (c1.compr1Mode == COMPR_1 && c2.compr1Mode == COMPR_2) {
            return false;
        }

        if (c1.compr2Mode == COMPR_2 && c2.compr2Mode == COMPR_1) {
            return true;
        } else if (c1.compr2Mode == COMPR_1 && c2.compr2Mode == COMPR_2) {
            return false;
        }

    }
    return c1.sum < c2.sum;
}

char StorageStrat::determineStrategy(long *v1, long *v2, const int size,
        const long nTermsInInput,
        const size_t nTermsClusterColumn,
        const bool useRowForLargeTables,
        Statistics &stats) {
    unsigned strat = 0;
    if (size < THRESHOLD_KEEP_MEMORY) {
        char nbytes1, nbytes2, nbytescount;
        long maxValue1 = 0;
        long maxValue2 = 0;

        long ngroups = 0;
        long maxGroupSize = 0;
        long currentGroupSize = 0;
        long prevEl = -1;
        for (int i = 0; i < size; i++) {
            if (v1[i] > maxValue1)
                maxValue1 = v1[i];
            if (v2[i] > maxValue2)
                maxValue2 = v2[i];

            if (v1[i] != prevEl) {
                prevEl = v1[i];
                currentGroupSize = 0;
                ngroups++;
            }
            currentGroupSize++;
            if (currentGroupSize > maxGroupSize) {
                maxGroupSize = currentGroupSize;
            }
        }

        //I have all info I need. Decide between row and cluster
        if (ngroups >= nTermsClusterColumn) {
            strat = setStorageType(strat, NEWCOLUMN_ITR);
            stats.nList2Strategies++;
        } else {
            if (maxGroupSize <= 255) {
                nbytescount = 1;
            } else {
                nbytescount = 4;
            }
            nbytes1 = Utils::numBytesFixedLength(maxValue1);
            char flagbytes1 = 3;
            if (nbytes1 == 1) {
                flagbytes1 = 0;
            } else if (nbytes1 == 2) {
                flagbytes1 = 1;
            } else if (nbytes1 < 5) {
                flagbytes1 = 2;
            }
            nbytes2 = Utils::numBytesFixedLength(maxValue2);
            char flagbytes2 = 3;
            if (nbytes2 == 1) {
                flagbytes2 = 0;
            } else if (nbytes2 == 2) {
                flagbytes2 = 1;
            } else if (nbytes2 < 5) {
                flagbytes2 = 2;
            }

            long totalSpaceRow = size * (nbytes1 + nbytes2);
            long totalSpaceCluster = ngroups * (nbytes1 + nbytescount) + nbytes2;
            if (totalSpaceRow < totalSpaceCluster) {
                strat = setStorageType(strat, NEWROW_ITR);
                strat = setBytesField1(strat, flagbytes1);
                strat = setBytesField2(strat, flagbytes2);
                stats.nListStrategies++;
            } else {
                strat = setStorageType(strat, NEWCLUSTER_ITR);
                strat = setBytesField1(strat, flagbytes1);
                strat = setBytesField2(strat, flagbytes2);
                if (nbytescount > 1) {
                    strat = strat | 1;
                }
                stats.nGroupStrategies++;
            }
        }
        stats.exact++;
    } else {
        if (useRowForLargeTables) {
            strat = setStorageType(strat, NEWROW_ITR);
            strat = setBytesField1(strat, 3);
            strat = setBytesField2(strat, 3);
            stats.nListStrategies++;
        } else {
            strat = setStorageType(strat, NEWCOLUMN_ITR);
            stats.nList2Strategies++;
        }
        stats.approximate++;
    }
    return strat;
}

char StorageStrat::determineStrategyOld(long *v1, long *v2, const int size,
        const long nTermsInInput,
        const size_t nTermsClusterColumn,
        Statistics &stats) {
    unsigned strat = 0;

    /***** DETERMINE TYPE STORAGE *****/
    int typeStorage = CLUSTER_ITR;
    //These counters assume compr2
    long listCounters1[2];
    long groupCounters1[2];
    long bytesSecondGroup = 0;
    listCounters1[0] = groupCounters1[0] = 0;
    listCounters1[1] = groupCounters1[1] = 0;
    long listCounters2 = 0, groupCounters2 = 0;

    //These counters assume compr1
    long listCounters1_2[2];
    long groupCounters1_2[2];
    long bytesSecondGroup_2 = 0;
    listCounters1_2[0] = groupCounters1_2[0] = 0;
    listCounters1_2[1] = groupCounters1_2[1] = 0;
    long listCounters2_2 = 0, groupCounters2_2 = 0;

    if (size < THRESHOLD_KEEP_MEMORY) {
        //What is the layout that compresses the table the most?
        size_t nUniqueFirstTerms = 0;

        //Calculate all options, and pick the best:
        long prevFirstValue = -1;
        long prevSecondValue = -1;

        for (int i = 0; i < size; i++) {
            for (int delta = 0; delta < 2; delta++) {
                //Size first term
                long value;
                if (delta == DIFFERENCE && prevFirstValue != -1) {
                    value = v1[i] - prevFirstValue;
                } else {
                    value = v1[i];
                }
                int bs = Utils::numBytes2(value);
                int bs2 = Utils::numBytes(value);

                listCounters1[delta] += bs;
                listCounters1_2[delta] += bs2;
                if (v1[i] != prevFirstValue) {
                    groupCounters1[delta] += bs + 1;
                    groupCounters1_2[delta] += bs2 + 1;

                    //Pointer is 4 bytes, and not one.
                    if (bytesSecondGroup > 255) {
                        groupCounters1[delta] += 3;
                    }
                    if (bytesSecondGroup_2 > 255) {
                        groupCounters1_2[delta] += 3;
                    }
                }
            }

            if (v1[i] != prevFirstValue) {
                prevFirstValue = v1[i];
                prevSecondValue = -1;
                nUniqueFirstTerms++;
                bytesSecondGroup = 0;
                bytesSecondGroup_2 = 0;
            }

            //Size second term: -- List
            long value = v2[i];
            int bs = Utils::numBytes2(value);
            int bs2 = Utils::numBytes(value);
            listCounters2 += bs;
            listCounters2_2 += bs2;

            //-- Group
            if (prevSecondValue != -1) {
                value = v2[i] - prevSecondValue;
                bs = Utils::numBytes2(value);
                bs2 = Utils::numBytes(value);
            }
            groupCounters2 += bs;
            bytesSecondGroup += bs;
            groupCounters2_2 += bs2;
            bytesSecondGroup_2 += bs2;
            prevSecondValue = v2[i];

            if (nUniqueFirstTerms > nTermsClusterColumn) {
                //Linear search is not possible on this set. Switch to column store
                typeStorage = COLUMN_ITR;
                stats.nList2Strategies++;
                strat = setCompr1(strat, COMPR_2);
                strat = setCompr2(strat, COMPR_2);
                stats.nFirstCompr2++;
                stats.nSecondCompr2++;
                stats.exact++;
                strat = setStorageType(strat, typeStorage);
                return (char) strat;
            }
        }

        //I might still have to add some bytes in case the last group was larger
        for (int delta = 0; delta < 2; delta++) {
            if (bytesSecondGroup > 255) {
                groupCounters1[delta] += 3;
            }
            if (bytesSecondGroup_2 > 255) {
                groupCounters1_2[delta] += 3;
            }
        }

        std::vector<Combinations> allCombinations;
        createAllCombinations(allCombinations, groupCounters1, listCounters1,
                groupCounters2, listCounters2, groupCounters1_2,
                listCounters1_2, groupCounters2_2, listCounters2_2);
        std::sort(allCombinations.begin(), allCombinations.end(), combinationSorter);

        //Smallest comnination
        Combinations c = allCombinations.front();
        if (c.type == CLUSTER_ITR) {
            typeStorage = CLUSTER_ITR;
            stats.nGroupStrategies++;
        } else {
            typeStorage = ROW_ITR;
            stats.nListStrategies++;
        }

        if (c.compr1Mode == COMPR_2) {
            strat = setCompr1(strat, COMPR_2);
            stats.nFirstCompr2++;
        } else {
            strat = setCompr1(strat, COMPR_1);
            stats.nFirstCompr1++;
        }

        if (c.compr2Mode == COMPR_2) {
            strat = setCompr2(strat, COMPR_2);
            stats.nSecondCompr2++;
        } else {
            strat = setCompr2(strat, COMPR_1);
            stats.nSecondCompr1++;
        }

        if (c.diffMode == DIFFERENCE) {
            strat = setDiff1(strat, DIFFERENCE);
            stats.diff++;
        } else {
            strat = setDiff1(strat, NO_DIFFERENCE);
            stats.nodiff++;
        }

        stats.exact++;
    } else {
        typeStorage = COLUMN_ITR;
        stats.nList2Strategies++;
        strat = setCompr1(strat, COMPR_2);
        strat = setCompr2(strat, COMPR_2);
        stats.nFirstCompr2++;
        stats.nSecondCompr2++;
        stats.approximate++;
    }

    strat = setStorageType(strat, typeStorage);
    return (char) strat;
}

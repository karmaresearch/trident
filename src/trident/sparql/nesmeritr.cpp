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


#include <trident/sparql/joins.h>

#include <iostream>

using namespace std;

#define GO_BACK(x) -x -1;

PairItr *NestedMergeJoinItr::getFirstIterator(Pattern p) {
    return q->getIterator(p.idx(), p.subject(), p.predicate(), p.object());
}

bool NestedMergeJoinItr::checkNext(PairItr *itr, bool shouldMoveToNext) {
    int64_t constraint1 = itr->getConstraint1();
    int64_t constraint2 = itr->getConstraint2();

    if (shouldMoveToNext) {
        if (!itr->hasNext()) {
            return false;
        }
        itr->next();
    }

    if (constraint1 >= 0 && itr->getValue1() != constraint1) {
        return false;
    }
    if (constraint2 >= 0 && itr->getValue2() != constraint2) {
        return false;
    }
    return true;
}

int64_t NestedMergeJoinItr::executePlan() {
    assert(outputTuples == 0);
    if (currentItr == NULL) {
        return 0;
    }

    while (true) {
        /* Get the next value of the current iterator. If the current
         * iterator does not have values anymore, then we move one level below.
         */

        if (!checkNext(currentItr, true)) {
            /* If the initial iterator is finished, then the join is terminated*/
            if (idxCurrentPattern == 0) {
                break;
            } else {
                //Go one pattern below
                idxCurrentPattern--;
                currentItr = iterators[idxCurrentPattern];
                currentVarsPos = plan->posVarsToCopyInIdx[idxCurrentPattern];
                nCurrentVarsPos = plan->nPosVarsToCopyInIdx[idxCurrentPattern];
                continue;
            }
        }

skipNext:

        //cerr << "Process pair " << idxCurrentPattern << "-" << currentItr->getValue1() << " "
        //     << currentItr->getValue2() << endl;

        /***** Fill the row with the variables of the pattern just read *****/
        idxCurrentRow = startingVarPerPattern[idxCurrentPattern];
        for (int i = 0; i < nCurrentVarsPos; ++i) {
            int64_t val = 0;
            switch (currentVarsPos[i]) {
            case 0:
                val = currentItr->getKey();
                break;
            case 1:
                val = currentItr->getValue1();
                break;
            case 2:
                val = currentItr->getValue2();
                break;
            }
            compressedRow[idxCurrentRow++] = val;
        }

        /***** IF THIS WAS THE LAST PATTERN ... *****/
        if (idxCurrentPattern == idxLastPattern) {
            //nElements++;
            for (int j = 0; j < sVarsToReturn; ++j) {
                outputResults->addValue(compressedRow[varsToReturn[j]]);
            }
            outputTuples++;
            if (outputTuples == maxTuplesInBuffer) {
                outputTuples = 0;
                return maxTuplesInBuffer;
            }
        } else {
            /***** PERFORM THE JOIN *****/
            //          timens::system_clock::time_point start =
            //                  timens::system_clock::now();
            int return_code = executeJoin(compressedRow, plan->patterns,
                                          idxCurrentPattern + 1, iterators,
                                          allJoins[idxCurrentPattern + 1],
                                          nJoins[idxCurrentPattern + 1]);
            //          timings[idxCurrentPattern + 1] += timens::system_clock::now()
            //                  - start;

            switch (return_code) {
            case JOIN_FAILED:
                //The join on these values has failed. Read the next value of
                //the current pattern
                continue;
            case JOIN_SUCCESSFUL:
                //Good, I can move on with reading the next pattern.
                idxCurrentPattern++;
                currentItr = iterators[idxCurrentPattern];
                currentVarsPos = plan->posVarsToCopyInIdx[idxCurrentPattern];
                nCurrentVarsPos = plan->nPosVarsToCopyInIdx[idxCurrentPattern];
                //The join has already read the first valid element for the
                //pattern. I don't need to call a next() in the next loop.
                goto skipNext;

            case NOMORE_JOIN:       //The join algorithm told me that it
                //is not possible to construct a join anymore...
                goto exit;
            default:
                // Move back of n patterns
                idxCurrentPattern = -return_code - 1;
                currentItr = iterators[idxCurrentPattern];
                currentVarsPos = plan->posVarsToCopyInIdx[idxCurrentPattern];
                nCurrentVarsPos = plan->nPosVarsToCopyInIdx[idxCurrentPattern];
            }
        }
    }

exit:

    cleanup();
    int64_t results = outputTuples;
    outputTuples = 0;
    currentItr = NULL;
    return results;
}

int NestedMergeJoinItr::executeJoin(int64_t *row, Pattern *patterns, int idxPattern,
                                    PairItr **iterators, JoinPoint *joins,
                                    const int nJoins) {

    /***** DETERMINE WHETHER WE NEED A NEW PATTERN *****/
    bool need_new_iterator = false;
    int performed_joins = 0;
    if (nJoins > 0) {
        if (joins[0].posIndex == 0) {
            if (row[joins[0].posRow] != joins[0].lastValue) {
                joins[0].lastValue = row[joins[0].posRow];
                need_new_iterator = true;
            } else {
                need_new_iterator = false;
                performed_joins++; //One join is already performed.
            }
        }
    } else {
        //If it is a cartesian product we always request a new pattern.
        need_new_iterator = true;
    }
    /***** RELEASE THE OLD ITERATOR IF I NEED TO *****/
    PairItr *itr = iterators[idxPattern];
    if (need_new_iterator && itr != NULL) {
        q->releaseItr(itr);
        itr = NULL;
    }
    /***** GET THE NEW ITERATOR *****/
    if (itr == NULL) {
        int64_t s = patterns[idxPattern].subject();
        int64_t p = patterns[idxPattern].predicate();
        int64_t o = patterns[idxPattern].object();

        //Replace first term
        performed_joins = 0;
        if (nJoins > 0 && joins[0].posIndex == 0) {
            performed_joins++;
            switch (joins[0].posPattern) {
            case 0:
                s = compressedRow[joins[0].posRow];
                break;
            case 1:
                p = compressedRow[joins[0].posRow];
                break;
            case 2:
                o = compressedRow[joins[0].posRow];
                break;
            }
        }
        itr = iterators[idxPattern] = q->getIterator(patterns[idxPattern].idx(), s, p,
                                             o);

        if (!itr->hasNext()) {
            //Release the iterator
            q->releaseItr(itr);
            iterators[idxPattern] = NULL;

            if (!need_new_iterator) {
                return NOMORE_JOIN;
            } else {
                return GO_BACK(joins[0].sourcePattern)
            }
        } else {
            itr->next();
        }

        itr->mark();

        //Otherwise, we should not do itr->next
        int leftJoins = nJoins - performed_joins;
        bool shouldMoveToNext = false;
        if (leftJoins > 0) {
            assert(leftJoins < 3);
            if (leftJoins == 1) {
                int i = performed_joins;
                int64_t v = compressedRow[joins[i].posRow];
                joins[i].lastValue = v;
                if (joins[i].posIndex == 1) {
                    itr->setConstraint1(v);
                    itr->moveto(v, 0);
                } else {
                    int64_t value1 = itr->getValue1();
                    assert(value1 != -1);
                    itr->setConstraint2(v);
                    itr->moveto(value1, v);
                }
            } else { //njoins == 2
                int64_t valtomove1 = -1;
                int64_t valtomove2 = -1;
                for (int i = performed_joins; i < nJoins; ++i) {
                    int64_t v = compressedRow[joins[i].posRow];
                    if (joins[i].posIndex == 1) {
                        valtomove1 = v;
                    } else {
                        valtomove2 = v;
                    }
                    joins[i].lastValue = v;
                }
                itr->setConstraint1(valtomove1);
                itr->setConstraint2(valtomove2);
                itr->moveto(valtomove1, valtomove2);
            }
            shouldMoveToNext = true;
        }

        /*bool first = true;
        for (int i = performed_joins; i < nJoins; ++i) {
            int64_t v = compressedRow[joins[i].posRow];
            if (joins[i].posIndex == 1) {
                if (!first) {
                    //I called moveto before. I must do hasNext()/next()
                    if (itr->hasNext()) {
                        itr->next();
                    } else {
                        break;
                    }
                }
                first = false;
                itr->setConstraint1(v);
                itr->moveto(v, 0);
            } else { // can only be 2
                if (!first) {
                    if (itr->hasNext()) {
                        itr->next();
                    } else {
                        break;
                    }
                }
                first = false;
                itr->setConstraint2(v);
                itr->moveto(itr->getValue1(), v);
            }
            joins[i].lastValue = v;
        }*/

        if (checkNext(itr, shouldMoveToNext)) {
            return JOIN_SUCCESSFUL;
        } else {
            return try_merge_join(iterators, idxPattern,
                                  joins + performed_joins, nJoins - performed_joins);
        }
    } else {
        //If there are new joins then we move the iterator.
        int remJoins = nJoins - performed_joins;
        if (remJoins == 0) {
            //I only need to reset the iterator
            itr->reset(0);
        } else if (remJoins == 1) {

            /*if (joins[performed_joins].lastValue > value_to_join_with
                    || !itr->hasNext() || itr->getValue1() == LONG_MIN) {
                itr->reset(0);
            } else {
                if (joins[performed_joins].lastValue == value_to_join_with) {
                    int nvars = patterns[idxPattern].getNVars();
                    if ((nvars == 2 && performed_joins == 0) || nvars == 3) {
                        itr->reset(1);
                    } else {
                        itr->reset(0);
                    }
                }
            }
            joins[performed_joins].lastValue = value_to_join_with;*/

            //The value is smaller than the current one. I need to reset
            //the iterator first...
            int64_t joinvalue = compressedRow[joins[performed_joins].posRow];
            if (joinvalue <= joins[performed_joins].lastValue) {
                itr->reset(0);
            }
            if (joins[performed_joins].posIndex == 1) {
                itr->setConstraint1(joinvalue);
                itr->moveto(joinvalue, 0);
            } else {
                assert(joins[performed_joins].posIndex == 2);
                itr->setConstraint2(joinvalue);
                assert(itr->getValue1() != -1);
                itr->moveto(itr->getValue1(), joinvalue);
            }
            joins[performed_joins].lastValue = joinvalue;

        } else if (remJoins == 2) {
            int64_t valuejoin1 = -1;
            int64_t valuejoin2 = -1;
            int64_t lastval1 = -1;
            int64_t lastval2 = -1;
            for (int i = performed_joins; i < performed_joins + 2; ++i) {
                if (joins[i].posIndex == 1) {
                    valuejoin1 = compressedRow[joins[i].posRow];
                    lastval1 = joins[i].lastValue;
                    joins[i].lastValue = valuejoin1;
                } else {
                    assert(joins[i].posIndex == 2);
                    valuejoin2 = compressedRow[joins[i].posRow];
                    lastval2 = joins[i].lastValue;
                    joins[i].lastValue = valuejoin2;
                }
            }
            assert(valuejoin1 != -1 && valuejoin2 != -1);
            if (valuejoin1 < lastval1 || (valuejoin1 == lastval1 &&
                                          valuejoin2 < lastval2)) {
                itr->reset(0);
            }

            itr->setConstraint1(valuejoin1);
            itr->setConstraint2(valuejoin2);
            itr->moveto(valuejoin1, valuejoin2);

        } else {
            assert(false); //Not considered
        }

        if (checkNext(itr, true)) {
            return JOIN_SUCCESSFUL;
        } else {
            return try_merge_join(iterators, idxPattern,
                                  joins + performed_joins, remJoins);
        }
    }
    return JOIN_SUCCESSFUL;
}

int NestedMergeJoinItr::try_merge_join(PairItr **iterators, int idxCurrentPattern,
                                       const JoinPoint *joins, const int nJoins) {
    if (nJoins == 0) {
        //I cannot do a merge join if there is no join to perform
        return JOIN_FAILED;
    } else if (nJoins == 1) {
        if (!joins[0].merge) {
            return JOIN_FAILED;
        }

        PairItr *sourceItr = iterators[joins[0].sourcePattern];
        PairItr *currentItr = iterators[idxCurrentPattern];

        int64_t value_to_join_with =
            joins[0].sourcePosIndex == 1 ?
            sourceItr->getValue1() : sourceItr->getValue2();
        int64_t current_value =
            joins[0].posIndex == 1 ?
            currentItr->getValue1() : currentItr->getValue2();
        //Two cases: the current value is smaller or larger.
        if (value_to_join_with < current_value) {
            if (joins[0].sourcePosIndex == 1) {
                sourceItr->moveto(current_value, 0);
            } else {
                sourceItr->moveto(sourceItr->getValue1(), current_value);
            }

            if (joins[0].posIndex == 1) {
                currentItr->setConstraint1(current_value);
            } else {
                currentItr->setConstraint2(current_value);
            }

            return GO_BACK(joins[0].sourcePattern);
        } else {
            //In this case, there are no more values in current Itr that
            //can produce a join. Because of this, we have to either go back to
            //the previous pattern than source or stop the join.
            if (joins[0].sourcePattern == 0) {
                return NOMORE_JOIN;
            } else {
                int idx = joins[0].sourcePattern - 1;
                return GO_BACK(idx);
            }
        }
        return JOIN_FAILED;

    } else {
        //nJoins == 2
        //Here we could further optimize the join
        return JOIN_FAILED;
    }
}

void NestedMergeJoinItr::cleanup() {
    // Release the iterators
    for (int i = 0; i < plan->nPatterns; ++i) {
        if (iterators[i] != NULL)
            q->releaseItr(iterators[i]);
    }

    // Remove references to the iterators
    int lastPattern = plan->nPatterns - 1;
    for (int i = 0; i <= lastPattern; ++i) {
        iterators[i] = NULL;
    }
}

void NestedMergeJoinItr::init(PairItr *firstIterator, TupleTable *outputR, int64_t limitOutputTuple) {
    /***** INIT VARIABLES *****/
    //nElements = 0; //# rows printed
    allJoins = plan->joins; //pointer to all joins to perform
    nJoins = plan->nJoins; //number of joins
    idxCurrentPattern = 0;
    currentVarsPos = plan->posVarsToCopyInIdx[idxCurrentPattern];
    nCurrentVarsPos = plan->nPosVarsToCopyInIdx[idxCurrentPattern];

    // Variables used to fulfill the values in the row to print
    startingVarPerPattern = plan->sizeRowsPatterns;
    idxLastPattern = plan->nPatterns - 1;
    idxCurrentRow = 0;

    // Variables used to print the output
    varsToReturn = plan->posVarsToReturn;
    sVarsToReturn = plan->nPosVarsToReturn;
    if (limitOutputTuple == 0) {
        maxTuplesInBuffer = OUTPUT_BUFFER_SIZE / sVarsToReturn;
    } else {
        maxTuplesInBuffer = limitOutputTuple;
    }
    outputTuples = 0;
    if (outputR == NULL) {
        this->outputResults = new TupleTable(sVarsToReturn);
        deleteOutputResults = true;
    } else {
        this->outputResults = outputR;
        deleteOutputResults = false;
    }

    for (int i = 0; i < plan->nPatterns; ++i) {
        iterators[i] = NULL;
    }
    for (int i = 0; i < MAX_N_PATTERNS; ++i) {
        compressedRow[i] = -1;
    }

    /***** GET FIRST ITERATOR *****/
    currentItr = iterators[0] = firstIterator;

    currentBuffer = NULL;
    remainingInBuffer = 0;
}

bool NestedMergeJoinItr::hasNext() {
    if (remainingInBuffer == 0) {
        outputResults->clear();
        int64_t results = executePlan();
        if (results > 0) {
            assert(outputResults->getNRows() > 0);
            currentBuffer = outputResults->getRow(0);
            remainingInBuffer = results * sVarsToReturn;
            return true;
        } else {
            return false;
        }
    }
    return true;
}

void NestedMergeJoinItr::next() {
    assert(remainingInBuffer > 0);
    remainingInBuffer -= sVarsToReturn;
}

uint64_t NestedMergeJoinItr::getElementAt(const int pos) {
    return currentBuffer[remainingInBuffer + pos];
}

size_t NestedMergeJoinItr::getTupleSize() {
    return sVarsToReturn;
}

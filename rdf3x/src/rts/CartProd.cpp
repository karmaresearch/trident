#include "rts/operator/CartProd.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"

#include <kognac/logs.h>

#include <iostream>

using namespace std;
//---------------------------------------------------------------------------
CartProd::CartProd(Operator* left,
        const vector<Register*>& leftTail,
        Operator* right, const vector<Register*>& rightTail,
        double expectedOutputCardinality,
        bool leftOptional, bool rightOptional,
        int bitset)
    : Operator(expectedOutputCardinality), left(left), right(right),
    leftTail(leftTail), rightTail(rightTail),
    leftOptional(leftOptional), rightOptional(rightOptional)
      // Constructor
{
}
//---------------------------------------------------------------------------
CartProd::~CartProd()
    // Destructor
{
    delete left;
    delete right;
}
//---------------------------------------------------------------------------
uint64_t CartProd::first()
    // Produce the first tuple
{
    //Need to store all the first entries in a vector
    if ((leftCount = left->first()) == 0) {
        if (leftOptional) {
            for(int i = 0; i < leftTail.size(); ++i) {
                leftTail[i]->value = ~0lu;
            }
            leftCount = 1;
        } else {
            return 0;
        }
    } else {
        if ((rightCount = right->first()) == 0) {
            if (rightOptional) {
                rightCount = 1;
                buffer.push_back(rightCount);
                for(int i = 0; i < rightTail.size(); ++i) {
                    buffer.push_back(~0lu);
                }
            } else {
                return 0;
            }
        } else {
            //Read all the tuples on the right side
            do {
                buffer.push_back(rightCount);
                for(int i = 0; i < rightTail.size(); ++i) {
                    buffer.push_back(rightTail[i]->value);
                }
            } while ((rightCount = right->next()) != 0);

            //Restore the first line
            for(int i = 0; i < rightTail.size(); ++i) {
                rightTail[i]->value = buffer[1 + i];
            }
            idxbuffer = 1 + rightTail.size();
        }
    }

    observedOutputCardinality = leftCount * buffer[0];
    return observedOutputCardinality;

    /*    currentIdx = -1;
    // Build the hash table if not already done
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    buildHashTableTask.run();
    std::chrono::duration<double> sec = std::chrono::system_clock::now()
    - start;
    LOG(INFOL) << "Runtime building hashtable = " << sec.count() * 1000 << " milliseconds";

    // Read the first tuple from the right side
    probePeekTask.run();
    if (((rightCount = probePeekTask.count) == 0) && !leftOptional) {
    return false;
    }

    // Setup the lookup
    hashTableIter = lookup(rightValue->value);
    joinSuccedeed = hashTableIter != NULL;
    if (rightOptional && joinSuccedeed) {
    collectedRightValues.insert(rightValue->value);
    }

    return next();*/
}
//---------------------------------------------------------------------------
uint64_t CartProd::next()
    // Produce the next tuple
{
    if (idxbuffer == buffer.size()) {
        //Move to the next left entry
        if ((leftCount = left->next()) == 0) {
            return 0;
        }
        idxbuffer = 0;
    }
    rightCount = buffer[idxbuffer++];
    for(int i = 0; i < rightTail.size(); ++i) {
        rightTail[i]->value = buffer[idxbuffer++];
    }
    observedOutputCardinality += leftCount * rightCount;
    return leftCount * rightCount;




    /*   if (currentIdx != -1) {
         Entry *e;
         while (currentIdx < (long)hashTable.size()) {
         if ((e = hashTable[currentIdx++])
         && !collectedRightValues.count(e->key)) {
         for (uint64_t index = 0, limit = leftTail.size();
         index < limit; ++index)
         leftTail[index]->value = e->values[index];
         return e->count;
         }
         }
         return false;
         }

    // Repeat until a match is found
    while (true) {
    // Still scanning the hash table?
    bool elementToProcess = false;
    for (; hashTableIter; hashTableIter = hashTableIter->next) {
    elementToProcess = true;
    uint64_t leftCount = hashTableIter->count;
    leftValue->value = hashTableIter->key;
    for (uint64_t index = 0, limit = leftTail.size(); index < limit; ++index)
    leftTail[index]->value = hashTableIter->values[index];
    hashTableIter = hashTableIter->next;

    uint64_t count = leftCount * rightCount;
    observedOutputCardinality += count;
    return count;
    }

    //First we finish all joins
    if (!elementToProcess && !joinSuccedeed && leftOptional) {
    joinSuccedeed = true;
    //Set the hash values to NULL
    for (uint64_t index = 0, limit = leftTail.size(); index < limit; ++index)
    leftTail[index]->value = ~0u;
    return rightCount;
    }

    // Read the next tuple from the right
    if ((rightCount = right->next()) == 0) {
    if (!rightOptional) {
    return false;
    } else {
    //Scan the all left table. return a NULL for each entry not joined
    currentIdx = 0;
    for (uint64_t index = 0, limit = rightTail.size();
    index < limit; ++index)
    rightTail[index]->value = ~0u;
    return next();
    }
    }

    hashTableIter = lookup(rightValue->value);
    joinSuccedeed = hashTableIter != NULL;
    if (rightOptional && joinSuccedeed) {
    collectedRightValues.insert(rightValue->value);
    }
    }*/
    return 0;
}
//---------------------------------------------------------------------------
void CartProd::print(PlanPrinter& out)
    // Print the operator tree. Debugging only.
{
    std::string s = "CartProd";
    if (leftOptional) s += "-leftOptional";
    if (rightOptional) s += "-rightOptional";
    out.beginOperator(s,expectedOutputCardinality,observedOutputCardinality);
    out.addMaterializationAnnotation(leftTail);
    out.addMaterializationAnnotation(rightTail);
    left->print(out);
    right->print(out);
    out.endOperator();
}
//---------------------------------------------------------------------------
void CartProd::addMergeHint(Register* /*reg1*/, Register* /*reg2*/)
    // Add a merge join hint
{
    // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void CartProd::getAsyncInputCandidates(Scheduler& scheduler)
    // Register parts of the tree that can be executed asynchronous
{
}
//---------------------------------------------------------------------------

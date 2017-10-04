#include "rts/operator/HashJoin.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"

#include <kognac/logs.h>

#include <iostream>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static inline uint64_t hash1(uint64_t key, uint64_t hashTableSize) {
    return key & (hashTableSize - 1);
}
static inline uint64_t hash2(uint64_t key, uint64_t hashTableSize) {
    return hashTableSize + ((key ^ (key >> 3)) & (hashTableSize - 1));
}
//---------------------------------------------------------------------------
void HashJoin::BuildHashTable::run()
    // Build the hash table
{
    if (done) return; // XXX support repeated executions under nested loop joins etc!

    // Prepare relevant domain informations
    Register* leftValue = join.leftValue;
    vector<Register*> domainRegs;
    if (leftValue->domain)
        domainRegs.push_back(leftValue);
    for (vector<Register*>::const_iterator iter = join.leftTail.begin(), limit = join.leftTail.end(); iter != limit; ++iter)
        if ((*iter)->domain)
            domainRegs.push_back(*iter);
    vector<ObservedDomainDescription> observedDomains;
    observedDomains.resize(domainRegs.size());

    // Build the hash table from the left side
    uint64_t hashTableSize = 1024;
    uint64_t tailLength = join.leftTail.size();
    join.hashTable.clear();
    join.hashTable.resize(2 * hashTableSize);
    for (uint64_t leftCount = join.left->first(); leftCount; leftCount = join.left->next()) {
        // Check the domain first
        bool joinCandidate = true;
        for (uint64_t index = 0, limit = domainRegs.size(); index < limit; ++index) {
            if (!domainRegs[index]->domain->couldQualify(domainRegs[index]->value)) {
                joinCandidate = false;
                break;
            }
            observedDomains[index].add(domainRegs[index]->value);
        }
        if (!joinCandidate)
            continue;
        // Compute the slots
        uint64_t leftKey = leftValue->value;
        uint64_t slot1 = hash1(leftKey, hashTableSize), slot2 = hash2(leftKey, hashTableSize);
        // LOG(DEBUGL) << "leftKey = " << leftKey;

        // Scan if the entry already exists
        Entry* e = join.hashTable[slot1];
        if ((!e) || (e->key != leftKey))
            e = join.hashTable[slot2];
        if (e && (e->key == leftKey)) {
            uint64_t ofs = (e == join.hashTable[slot1]) ? slot1 : slot2;
            bool match = false;
            for (Entry* iter = e; iter; iter = iter->next)
                if (leftKey == iter->key) {
                    // Tuple already in the table?
                    match = true;
                    for (uint64_t index2 = 0; index2 < tailLength; index2++)
                        if (join.leftTail[index2]->value != iter->values[index2]) {
                            match = false;
                            break;
                        }
                    // Then aggregate
                    if (match) {
                        iter->count += leftCount;
                        break;
                    }
                }
            if (match)
                continue;

            // Append to the current bucket
            e = join.entryPool.alloc();
            e->next = join.hashTable[ofs];
            join.hashTable[ofs] = e;
            e->key = leftKey;
            e->count = leftCount;
            for (uint64_t index2 = 0; index2 < tailLength; index2++)
                e->values[index2] = join.leftTail[index2]->value;
            continue;
        }

        // Create a new tuple
        join.keys.push_back(leftKey);
        e = join.entryPool.alloc();
        e->next = 0;
        e->key = leftKey;
        e->count = leftCount;
        for (uint64_t index2 = 0; index2 < tailLength; index2++)
            e->values[index2] = join.leftTail[index2]->value;

        // And insert it
        join.insert(e);
        hashTableSize = join.hashTable.size() / 2;
    }

    // Update the domains
    for (uint64_t index = 0, limit = domainRegs.size(); index < limit; ++index)
        domainRegs[index]->domain->restrictTo(observedDomains[index]);
    if (join.bitset != 0) {
        join.right->setHashKeys(&join.keys, join.bitset);
    }

    done = true;
}
//---------------------------------------------------------------------------
void HashJoin::ProbePeek::run()
    // Produce the first tuple from the probe side
{
    if (done) return; // XXX support repeated executions under nested loop joins etc!

    count = join.right->first();
    done = true;
}
//---------------------------------------------------------------------------
HashJoin::HashJoin(Operator* left, Register* leftValue, const vector<Register*>& leftTail, Operator* right, Register* rightValue, const vector<Register*>& rightTail, double hashPriority,
        double probePriority, double expectedOutputCardinality, bool leftOptional, bool rightOptional, int bitset)
    : Operator(expectedOutputCardinality), left(left), right(right), leftValue(leftValue), rightValue(rightValue),
    leftTail(leftTail), rightTail(rightTail), entryPool(leftTail.size() * sizeof(uint64_t)),
    bitset(bitset), buildHashTableTask(*this), probePeekTask(*this), hashPriority(hashPriority), probePriority(probePriority),
    leftOptional(leftOptional), rightOptional(rightOptional)
      // Constructor
{
}
//---------------------------------------------------------------------------
HashJoin::~HashJoin()
    // Destructor
{
    delete left;
    delete right;
}
//---------------------------------------------------------------------------
void HashJoin::insert(Entry* e)
    // Insert into the hash table
{
    uint64_t hashTableSize = hashTable.size() / 2;
    // Try to insert
    bool firstTable = true;
    for (uint64_t index = 0; index < hashTableSize; index++) {
        uint64_t slot = firstTable ? hash1(e->key, hashTableSize) : hash2(e->key, hashTableSize);
        swap(e, hashTable[slot]);
        if (!e)
            return;
        firstTable = !firstTable;
    }

    // No place found, rehash
    vector<Entry*> oldTable;
    oldTable.resize(4 * hashTableSize);
    swap(hashTable, oldTable);
    for (vector<Entry*>::const_iterator iter = oldTable.begin(), limit = oldTable.end(); iter != limit; ++iter)
        if (*iter)
            insert(*iter);
    insert(e);
}
//---------------------------------------------------------------------------
HashJoin::Entry* HashJoin::lookup(uint64_t key)
    // Search an entry in the hash table
{
    uint64_t hashTableSize = hashTable.size() / 2;
    Entry* e = hashTable[hash1(key, hashTableSize)];
    if (e && (e->key == key))
        return e;
    e = hashTable[hash2(key, hashTableSize)];
    if (e && (e->key == key))
        return e;
    return 0;
}
//---------------------------------------------------------------------------
uint64_t HashJoin::first()
    // Produce the first tuple
{
    currentIdx = -1;
    observedOutputCardinality = 0;
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

    return next();
}
//---------------------------------------------------------------------------
uint64_t HashJoin::next()
    // Produce the next tuple
{
    if (currentIdx != -1) {
        Entry *e;
        while (currentIdx < (long)hashTable.size()) {
            if ((e = hashTable[currentIdx++])
                    && !collectedRightValues.count(e->key)) {
                for (uint64_t index = 0, limit = leftTail.size();
                        index < limit; ++index)
                    leftTail[index]->value = e->values[index];
                return 1;
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
    }
}
//---------------------------------------------------------------------------
void HashJoin::print(PlanPrinter& out)
    // Print the operator tree. Debugging only.
{
    out.beginOperator("HashJoin", expectedOutputCardinality, observedOutputCardinality);
    out.addEqualPredicateAnnotation(leftValue, rightValue);
    out.addMaterializationAnnotation(leftTail);
    out.addMaterializationAnnotation(rightTail);
    left->print(out);
    right->print(out);
    out.endOperator();
}
//---------------------------------------------------------------------------
void HashJoin::addMergeHint(Register* /*reg1*/, Register* /*reg2*/)
    // Add a merge join hint
{
    // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void HashJoin::getAsyncInputCandidates(Scheduler& scheduler)
    // Register parts of the tree that can be executed asynchronous
{
    uint64_t p1 = scheduler.getRegisteredPoints();
    left->getAsyncInputCandidates(scheduler);
    scheduler.registerAsyncPoint(buildHashTableTask, 0, hashPriority, p1);

    uint64_t p2 = scheduler.getRegisteredPoints();
    right->getAsyncInputCandidates(scheduler);
    scheduler.registerAsyncPoint(probePeekTask, 1, probePriority, p2);
}
//---------------------------------------------------------------------------

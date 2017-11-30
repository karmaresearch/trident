#include "rts/operator/ResultsPrinter.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/QueryDict.hpp"
#include "rts/runtime/TemporaryDictionary.hpp"
#include "infra/util/Type.hpp"

#include <trident/kb/dictmgmt.h>

#include <iostream>
#include <map>
#include <set>
#include <cstring>
#include <sstream>
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
ResultsPrinter::ResultsPrinter(Runtime& runtime, Operator* input, const vector<Register*>& output, DuplicateHandling duplicateHandling, uint64_t limit, bool silent)
    : Operator(1), output(output), input(input), runtime(runtime), dictionary(runtime.getDatabase()), duplicateHandling(duplicateHandling), outputMode(DefaultOutput), limit(limit), silent(silent), nrows(0), jsonoutput(NULL), outputset(NULL)
      // Constructor
{
}
//---------------------------------------------------------------------------
ResultsPrinter::~ResultsPrinter()
    // Destructor
{
    delete input;
}
//---------------------------------------------------------------------------
namespace {
    //---------------------------------------------------------------------------
    void CacheEntry::printValue(ostream &out, bool escape) const
        // Print the raw value
    {
        if (escape) {
            for (const char* iter = start, *limit = stop; iter != limit; ++iter) {
                char c = *iter;
                if ((c == ' ') || (c == '\n') || (c == '\\'))
                    out << '\\';
                out << c;
            }
        } else {
            for (const char* iter = start, *limit = stop; iter != limit; ++iter)
                out << *iter;
        }
    }
    //---------------------------------------------------------------------------
    void CacheEntry::print(ostream &out, const map<uint64_t, CacheEntry>& stringCache, bool escape) const
        // Print it
    {
        switch (type) {
            case Type::URI:
                out << '<';
                printValue(out, escape);
                out << '>';
                break;
            case Type::Literal:
                out << '"';
                printValue(out, escape);
                out << '"';
                break;
            case Type::CustomLanguage:
                out << '"';
                printValue(out, escape);
                out << "\"@";
                (*stringCache.find(subType)).second.printValue(out, escape);
                break;
            case Type::CustomType:
                out << '"';
                printValue(out, escape);
                out << "\"^^<";
                (*stringCache.find(subType)).second.printValue(out, escape);
                out << ">";
                break;
            case Type::String:
                out << '"';
                printValue(out, escape);
                out << "\"^^<http://www.w3.org/2001/XMLSchema#string>";
                break;
            case Type::Integer:
                out << '"';
                printValue(out, escape);
                out << "\"^^<http://www.w3.org/2001/XMLSchema#integer>";
                break;
            case Type::Decimal:
                out << '"';
                printValue(out, escape);
                out << "\"^^<http://www.w3.org/2001/XMLSchema#decimal>";
                break;
            case Type::Double:
                out << '"';
                printValue(out, escape);
                out << "\"^^<http://www.w3.org/2001/XMLSchema#double>";
                break;
            case Type::Boolean:
                out << '"';
                printValue(out, escape);
                out << "\"^^<http://www.w3.org/2001/XMLSchema#boolean>";
                break;
            case Type::Date:
                out << '"';
                printValue(out, escape);
                out << '"';
                break;
        }
    }
    //---------------------------------------------------------------------------
    void CacheEntry::print(const map<uint64_t, CacheEntry>& stringCache, bool escape) const {
        print(cout, stringCache, escape);
    }
    //---------------------------------------------------------------------------
    string CacheEntry::tostring(const map<uint64_t, CacheEntry>& stringCache, bool escape) const {
        ostringstream os;
        print(os, stringCache, escape);
        return os.str();
    }
    //---------------------------------------------------------------------------
    static void printResult(map<uint64_t, CacheEntry>& stringCache, vector<uint64_t>::const_iterator start, vector<uint64_t>::const_iterator stop, bool escape)
        // Print a result row
    {
        if (start == stop) return;
        if (!~(*start))
            cout << "NULL";
        else {
            if (!DictMgmt::isnumeric(*start)) {
                stringCache[*start].print(stringCache, escape);
            } else {
                cout << DictMgmt::tostr(*start);
            }
        }
        for (++start; start != stop; ++start) {
            cout << '\t';
            if (!~(*start))
                cout << "NULL";
            else {
                if (!DictMgmt::isnumeric(*start)) {
                    stringCache[*start].print(stringCache, escape);
                } else {
                    cout << DictMgmt::tostr(*start);
                }
            }
        }
    }
};
//---------------------------------------------------------------------------
void formatJSONRow(const std::vector<std::string> &columns,
        map<uint64_t, CacheEntry> &stringCache,
        vector<uint64_t>::const_iterator start,
        vector<uint64_t>::const_iterator stop,
        JSON *output) {
    if (start == stop) return;
    JSON row;
    if (start != stop) {
        int idx = 0;
        JSON fbinding;
        if (!~(*start)) {
            fbinding.put("type", "literal");
            fbinding.put("value", "NULL");
        } else {
            std::string el = stringCache[*start].tostring(stringCache, false);
            if (el[0] == '<' && el.size() > 2) {
                fbinding.put("type", "uri");
                fbinding.put("value", el.substr(1, el.size() - 2));
            } else {
                fbinding.put("type", "literal");
                fbinding.put("value", el);
            }
        }
        row.add_child(columns[idx++], fbinding);

        for (++start; start != stop; ++start) {
            JSON binding;
            if (!~(*start)) {
                binding.put("type", "literal");
                binding.put("value", "NULL");
            } else {
                std::string el = stringCache[*start].tostring(stringCache, false);
                if (el[0] == '<' && el.size() > 2) {
                    binding.put("type", "uri");
                    binding.put("value", el.substr(1, el.size() - 2));
                } else {
                    binding.put("type", "literal");
                    binding.put("value", el);
                }
            }
            row.add_child(columns[idx++], binding);
        }
    }
    output->push_back(row);
}
//---------------------------------------------------------------------------
void ResultsPrinter::formatJSON(const std::vector<string> &columns,
        vector<uint64_t> &results,
        map<uint64_t, CacheEntry> &stringCache,
        ResultsPrinter::DuplicateHandling duplicateHandling,
        JSON *output) {
    const int ncolumns = columns.size();
    if (duplicateHandling == ResultsPrinter::ExpandDuplicates) {
        for (vector<uint64_t>::const_iterator iter = results.begin(),
                limit = results.end(); iter != limit;) {
            uint64_t count = *iter;
            ++iter;
            for (uint64_t index = 0; index < count; index++) {
                nrows++;
                formatJSONRow(columns, stringCache, iter, iter + ncolumns, output);
            }
            iter += ncolumns;
        }
    } else {
        // No, reduced, count, or duplicates
        for (vector<uint64_t>::const_iterator iter = results.begin(),
                limit = results.end(); iter != limit;) {
            //uint64_t count = *iter;
            ++iter;
            nrows++;
            formatJSONRow(columns, stringCache, iter, iter + ncolumns, output);
            iter += ncolumns;
        }
    }
}
//---------------------------------------------------------------------------
uint64_t ResultsPrinter::first()
    // Produce the first tuple
{
    observedOutputCardinality = 1;

    // Empty input?
    uint64_t count;
    if ((count = input->first()) == 0) {
        if ((!silent) && (outputMode != Embedded))
            cout << "<empty result>" << endl;
        return 1;
    }

    if (outputset) {
        //Copy the values into the output set
        uint64_t minCount = (duplicateHandling == ShowDuplicates) ? 2 : 1;
        uint64_t entryCount = 0;
        do {
            if (count < minCount) continue;
            uint64_t id = output[prjId]->value;
            outputset->insert(id);
            if ((++entryCount) >= this->limit) break;
        } while ((count = input->next()) != 0);
        return 1;
    }

    if (silent && !jsonoutput) {
        //Count the rows and output a single line
        do {
            nrows += count;
        } while ((count = input->next()) != 0 && nrows < this->limit);
        cout << "<skipped " << nrows << " rows>" << endl;
        return 1;
    }

    // Collect the values
    vector<uint64_t> results;
    map<uint64_t, CacheEntry> stringCache;
    uint64_t minCount = (duplicateHandling == ShowDuplicates) ? 2 : 1;
    uint64_t entryCount = 0;
    do {
        if (count < minCount) continue;
        results.push_back(count);
        for (vector<Register*>::const_iterator iter = output.begin(), limit = output.end(); iter != limit; ++iter) {
            uint64_t id = (*iter)->value;
            results.push_back(id);
            if (~id && !DictMgmt::isnumeric(id)) stringCache[id];
        }
        if ((++entryCount) >= this->limit) break;
    } while ((count = input->next()) != 0);

    //Data structures to maintain a permanent copy of the strings
    std::vector<std::unique_ptr<char[]>> buf_all;
    const size_t buf_max = 10 * 1024 * 1024;
    std::unique_ptr<char[]> buf_current(new char[buf_max]);
    size_t buf_size = 0;

    // Lookup the strings
    set<unsigned> subTypes;
    TemporaryDictionary* tempDict = runtime.hasTemporaryDictionary() ?
        (&runtime.getTemporaryDictionary()) : 0;
    QueryDict *dictQuery = runtime.getQueryDict();
    if (dictQuery && dictQuery->isEmpty()) dictQuery = NULL;
    for (map<uint64_t, CacheEntry>::iterator iter = stringCache.begin(),
            limit = stringCache.end(); iter != limit; ++iter) {
        CacheEntry& c = (*iter).second;

        if (dictQuery && dictQuery->hasID(iter->first)) {
            //I need to set c.start and c.stop
            std::pair<char*, char*> pair = dictQuery->getStringBoundaries(iter->first);
            c.start = pair.first;
            c.stop = pair.second;
            c.type = Type::Literal;
        } else {
            if (tempDict)
                tempDict->lookupById((*iter).first, c.start, c.stop, c.type, c.subType);
            else
                // if (diffIndex)
                //diffIndex->lookupById((*iter).first, c.start, c.stop, c.type, c.subType);
                //else
                dictionary.lookupById((*iter).first, c.start, c.stop, c.type, c.subType);

            //Copy the text in a permanent data structure
            const size_t len = c.stop - c.start;
            if (len > buf_max - buf_size) {
                buf_all.push_back(std::move(buf_current));
                buf_current = std::unique_ptr<char[]>(new char[buf_max]);
                buf_size = 0;
            }
            memcpy(buf_current.get() + buf_size, c.start, len);
            c.start = buf_current.get() + buf_size;
            c.stop = c.start + len;
            buf_size += len;
            //Copying done
        }

        if (Type::hasSubType(c.type))
            subTypes.insert(c.subType);
    }

    for (set<unsigned>::const_iterator iter = subTypes.begin(),
            limit = subTypes.end(); iter != limit; ++iter) {
        CacheEntry& c = stringCache[*iter];
        if (tempDict)
            tempDict->lookupById(*iter, c.start, c.stop, c.type, c.subType);
        //else if (diffIndex)
        //    diffIndex->lookupById(*iter, c.start, c.stop, c.type, c.subType);
        else
            dictionary.lookupById(*iter, c.start, c.stop, c.type, c.subType);

        //Copy the text in a permanent data structure
        const size_t len = c.stop - c.start;
        if (len > buf_max - buf_size) {
            buf_all.push_back(std::move(buf_current));
            buf_current = std::unique_ptr<char[]>(new char[buf_max]);
            buf_size = 0;
        }
        memcpy(buf_current.get() + buf_size, c.start, len);
        c.start = buf_current.get() + buf_size;
        c.stop = c.start + len;
        buf_size += len;
        //Copying done
    }

    if (jsonoutput) {
        //Format the output in JSON format
        formatJSON(this->jsonvars, results, stringCache, duplicateHandling,
                jsonoutput);
    }

    // Skip printing the results?
    if (silent) {
        return 1;
    }

    // Expand duplicates?
    uint64_t columns = output.size();
    if (duplicateHandling == ExpandDuplicates) {
        for (vector<uint64_t>::const_iterator iter = results.begin(),
                limit = results.end(); iter != limit;) {
            uint64_t count = *iter;
            ++iter;
            for (uint64_t index = 0; index < count; index++) {
                nrows++;
                printResult(stringCache, iter, iter + columns, (outputMode == Embedded));
                cout << '\n';
            }
            iter += columns;
        }
    } else {
        // No, reduced, count, or duplicates
        for (vector<uint64_t>::const_iterator iter = results.begin(), limit = results.end(); iter != limit;) {
            uint64_t count = *iter;
            ++iter;
            nrows++;
            printResult(stringCache, iter, iter + columns, (outputMode == Embedded));
            if (duplicateHandling != ReduceDuplicates)
                cout << " " << count;
            cout << '\n';
            iter += columns;
        }
    }

    return 1;
}
//---------------------------------------------------------------------------
uint64_t ResultsPrinter::next()
    // Produce the next tuple
{
    return false;
}
//---------------------------------------------------------------------------
void ResultsPrinter::print(PlanPrinter& out)
    // Print the operator tree. Debugging only.
{
    out.beginOperator("ResultsPrinter", expectedOutputCardinality, observedOutputCardinality);
    out.addMaterializationAnnotation(output);
    input->print(out);
    out.endOperator();
}
//---------------------------------------------------------------------------
void ResultsPrinter::addMergeHint(Register* /*reg1*/, Register* /*reg2*/)
    // Add a merge join hint
{
    // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void ResultsPrinter::getAsyncInputCandidates(Scheduler& scheduler)
    // Register parts of the tree that can be executed asynchronous
{
    input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------

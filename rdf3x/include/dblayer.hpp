#ifndef _DBLAYER_HPP
#define _DBLAYER_HPP

#include <infra/util/Type.hpp>
#include <string>
#include <memory>
#include <vector>

#include <inttypes.h>

//#define AGGR_NO 0
//#define AGGR_SKIP_LAST 1
//#define AGGR_SKIP_2LAST 2

class DBLayer {
public:

    typedef enum { AGGR_NO, AGGR_SKIP_LAST, AGGR_SKIP_2LAST } Aggr_t;

    class Scan {
    public:
        virtual uint64_t getValue1() = 0;

        virtual uint64_t getValue2() = 0;

        virtual uint64_t getValue3() = 0;

        virtual uint64_t getCount() = 0;

        virtual bool next() = 0;

        virtual bool first() = 0;

        virtual bool first(uint64_t, bool) = 0;

        virtual bool first(uint64_t, bool, uint64_t, bool) = 0;

        virtual bool first(uint64_t, bool, uint64_t, bool, uint64_t, bool) = 0;

        virtual ~Scan() {}
    };

    class Hint {
    private:
	std::vector<uint64_t> *hashKeys = NULL;	// Keys of a hashjoin. Significant for the right iterator.
	int varbitset = 0;

    public:
	std::vector<uint64_t> *getKeys(int *bitset) {
	    if (bitset != NULL) {
		*bitset = varbitset;
	    }
	    return hashKeys;
	}

	void setKeys(std::vector<uint64_t> *keys, int bitset) {
	    hashKeys = keys;
	    varbitset = bitset;
	}

        virtual void next(uint64_t& value1,
                          uint64_t& value2,
                          uint64_t& value3) {
            throw 10;
        }

        virtual void next(uint64_t& value1,
                          uint64_t& value2) {
            throw 10;
        }

        virtual void next(uint64_t& value1) {
            throw 10;
        }
    };

    enum DataOrder {
        Order_Subject_Predicate_Object = 0,
        Order_Subject_Object_Predicate,
        Order_Object_Predicate_Subject,
        Order_Object_Subject_Predicate,
        Order_Predicate_Subject_Object,
        Order_Predicate_Object_Subject,
        Order_No_Order_SPO,
        Order_No_Order_SOP,
        Order_No_Order_OPS,
        Order_No_Order_OSP,
        Order_No_Order_POS,
        Order_No_Order_PSO,
    };

    virtual bool lookup(const std::string& text,
                        ::Type::ID type,
                        unsigned subType,
                        uint64_t& id) = 0;

    virtual bool lookupById(uint64_t id,
                            //char *output,
                            //size_t &length,
                            const char*& start,
                            const char*& stop,
                            ::Type::ID& type,
                            unsigned& subType) = 0;

    virtual bool lookupById(uint64_t id,
                            char *output,
                            size_t &length,
                            ::Type::ID& type,
                            unsigned& subType) = 0;

    virtual uint64_t getNextId() = 0;

    virtual double getScanCost(DBLayer::DataOrder order,
                               uint64_t value1,
                               uint64_t value1C,
                               uint64_t value2,
                               uint64_t value2C,
                               uint64_t value3,
                               uint64_t value3C) = 0;

    virtual double getScanCost(DBLayer::DataOrder order,
                               uint64_t value1,
                               uint64_t value1C,
                               uint64_t value2,
                               uint64_t value2C) = 0;

    virtual double getJoinSelectivity(bool valueL1,
                                      uint64_t value1CL,
                                      bool value2L,
                                      uint64_t value2CL,
                                      bool value3L,
                                      uint64_t value3CL,
                                      bool value1R,
                                      uint64_t value1CR,
                                      bool value2R,
                                      uint64_t value2CR,
                                      bool value3R,
                                      uint64_t value3CR) = 0;

    virtual double getScanCost(DBLayer::DataOrder order,
                               uint64_t value1,
                               uint64_t value1C) = 0;

    virtual uint64_t getCardinality(uint64_t c1,
                                    uint64_t c2,
                                    uint64_t c3) = 0;

    virtual uint64_t getCardinality() = 0;

    virtual std::unique_ptr<DBLayer::Scan> getScan(const DataOrder order,
            const Aggr_t aggr,
            Hint *hint) = 0;

    virtual ~DBLayer() {
    }
};

#endif

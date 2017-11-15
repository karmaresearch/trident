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


#ifndef _FACTORYTABLE_H
#define _FACTORYTABLE_H

#include <kognac/factory.h>
#include <kognac/logs.h>
#include <trident/binarytables/newcolumntable.h>
#include <trident/binarytables/newrowtable.h>
#include <trident/binarytables/newclustertable.h>
#include <trident/binarytables/binarytablereaders.h>
#include <trident/binarytables/newtable.h>

#include <kognac/logs.h>

struct FactoryNewColumnTable {
    static void get12Reader(const char nbytes1,
            const char nbytes2,
            void (**output)(const uint64_t ,
                const uint8_t ,
                const char *,
                const uint64_t,
                uint64_t &,
                uint64_t &)) {
        switch (nbytes1) {
            case 1:
                switch (nbytes2) {
                    case 1:
                        *output = &NewColumnTable::s_getValue12AtRow<1,1>;
                        break;
                    case 2:
                        *output = &NewColumnTable::s_getValue12AtRow<1,2>;
                        break;
                    case 3:
                        *output = &NewColumnTable::s_getValue12AtRow<1,3>;
                        break;
                    case 4:
                        *output = &NewColumnTable::s_getValue12AtRow<1,4>;
                        break;
                    case 5:
                        *output = &NewColumnTable::s_getValue12AtRow<1,5>;
                        break;
                    default:
                        LOG(ERRORL) << "Not supported";
                        throw 10;
                }
                break;
            case 2:
                switch (nbytes2) {
                    case 1:
                        *output = &NewColumnTable::s_getValue12AtRow<2,1>;
                        break;
                    case 2:
                        *output = &NewColumnTable::s_getValue12AtRow<2,2>;
                        break;
                    case 3:
                        *output = &NewColumnTable::s_getValue12AtRow<2,3>;
                        break;
                    case 4:
                        *output = &NewColumnTable::s_getValue12AtRow<2,4>;
                        break;
                    case 5:
                        *output = &NewColumnTable::s_getValue12AtRow<2,5>;
                        break;
                    default:
                        LOG(ERRORL) << "Not supported";
                        throw 10;
                }
                break;
            case 3:
                switch (nbytes2) {
                    case 1:
                        *output = &NewColumnTable::s_getValue12AtRow<3,1>;
                        break;
                    case 2:
                        *output = &NewColumnTable::s_getValue12AtRow<3,2>;
                        break;
                    case 3:
                        *output = &NewColumnTable::s_getValue12AtRow<3,3>;
                        break;
                    case 4:
                        *output = &NewColumnTable::s_getValue12AtRow<3,4>;
                        break;
                    case 5:
                        *output = &NewColumnTable::s_getValue12AtRow<3,5>;
                        break;
                    default:
                        LOG(ERRORL) << "Not supported";
                        throw 10;
                }
                break;
            case 4:
                switch (nbytes2) {
                    case 1:
                        *output = &NewColumnTable::s_getValue12AtRow<4,1>;
                        break;
                    case 2:
                        *output = &NewColumnTable::s_getValue12AtRow<4,2>;
                        break;
                    case 3:
                        *output = &NewColumnTable::s_getValue12AtRow<4,3>;
                        break;
                    case 4:
                        *output = &NewColumnTable::s_getValue12AtRow<4,4>;
                        break;
                    case 5:
                        *output = &NewColumnTable::s_getValue12AtRow<4,5>;
                        break;
                    default:
                        LOG(ERRORL) << "Not supported";
                        throw 10;
                }
                break;
            case 5:
                switch (nbytes2) {
                    case 1:
                        *output = &NewColumnTable::s_getValue12AtRow<5,1>;
                        break;
                    case 2:
                        *output = &NewColumnTable::s_getValue12AtRow<5,2>;
                        break;
                    case 3:
                        *output = &NewColumnTable::s_getValue12AtRow<5,3>;
                        break;
                    case 4:
                        *output = &NewColumnTable::s_getValue12AtRow<5,4>;
                        break;
                    case 5:
                        *output = &NewColumnTable::s_getValue12AtRow<5,5>;
                        break;
                    default:
                        LOG(ERRORL) << "Not supported";
                        throw 10;
                }
                break;
            default:
                LOG(ERRORL) << "Not supported";
                throw 10;
        }
    }
};

struct FactoryNewClusterTable {
    Factory<NewClusterTable<ByteReader, ByteReader, ByteReader>> f111;
    Factory<NewClusterTable<ByteReader, ShortReader, ByteReader>> f121;
    Factory<NewClusterTable<ByteReader, IntReader, ByteReader>> f141;
    Factory<NewClusterTable<ByteReader, LongIntReader, ByteReader>> f181;

    Factory<NewClusterTable<ShortReader, ByteReader, ByteReader>> f211;
    Factory<NewClusterTable<ShortReader, ShortReader, ByteReader>> f221;
    Factory<NewClusterTable<ShortReader, IntReader, ByteReader>> f241;
    Factory<NewClusterTable<ShortReader, LongIntReader, ByteReader>> f281;

    Factory<NewClusterTable<IntReader, ByteReader, ByteReader>> f411;
    Factory<NewClusterTable<IntReader, ShortReader, ByteReader>> f421;
    Factory<NewClusterTable<IntReader, IntReader, ByteReader>> f441;
    Factory<NewClusterTable<IntReader, LongIntReader, ByteReader>> f481;

    Factory<NewClusterTable<LongIntReader, ByteReader, ByteReader>> f811;
    Factory<NewClusterTable<LongIntReader, ShortReader, ByteReader>> f821;
    Factory<NewClusterTable<LongIntReader, IntReader, ByteReader>> f841;
    Factory<NewClusterTable<LongIntReader, LongIntReader, ByteReader>> f881;

    Factory<NewClusterTable<ByteReader, ByteReader, IntReader>> f112;
    Factory<NewClusterTable<ByteReader, ShortReader, IntReader>> f122;
    Factory<NewClusterTable<ByteReader, IntReader, IntReader>> f142;
    Factory<NewClusterTable<ByteReader, LongIntReader, IntReader>> f182;

    Factory<NewClusterTable<ShortReader, ByteReader, IntReader>> f212;
    Factory<NewClusterTable<ShortReader, ShortReader, IntReader>> f222;
    Factory<NewClusterTable<ShortReader, IntReader, IntReader>> f242;
    Factory<NewClusterTable<ShortReader, LongIntReader, IntReader>> f282;

    Factory<NewClusterTable<IntReader, ByteReader, IntReader>> f412;
    Factory<NewClusterTable<IntReader, ShortReader, IntReader>> f422;
    Factory<NewClusterTable<IntReader, IntReader, IntReader>> f442;
    Factory<NewClusterTable<IntReader, LongIntReader, IntReader>> f482;

    Factory<NewClusterTable<LongIntReader, ByteReader, IntReader>> f812;
    Factory<NewClusterTable<LongIntReader, ShortReader, IntReader>> f822;
    Factory<NewClusterTable<LongIntReader, IntReader, IntReader>> f842;
    Factory<NewClusterTable<LongIntReader, LongIntReader, IntReader>> f882;

    static void getReaderName(const char nbytes1,
            const char nbytes2,
            const char ncountbytes) {
        switch (nbytes1) {
            case 0:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<ByteReader, ByteReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<ByteReader, ByteReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 1:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<ByteReader, ShortReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<ByteReader, ShortReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 2:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<ByteReader, IntReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<ByteReader, IntReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 3:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<ByteReader, LongIntReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<ByteReader, LongIntReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                }
                break;
            case 1:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<ShortReader, ByteReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<ShortReader, ByteReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 1:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<ShortReader, ShortReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<ShortReader, ShortReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 2:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<ShortReader, IntReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<ShortReader, IntReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 3:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<ShortReader, LongIntReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<ShortReader, LongIntReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                }
                break;
            case 2:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<IntReader, ByteReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<IntReader, ByteReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 1:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<IntReader, ShortReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<IntReader, ShortReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 2:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<IntReader, IntReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<IntReader, IntReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 3:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<IntReader, LongIntReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<IntReader, LongIntReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                }
                break;
            case 3:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<LongIntReader, ByteReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<LongIntReader, ByteReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 1:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<LongIntReader, ShortReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<LongIntReader, ShortReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 2:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<LongIntReader, IntReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<LongIntReader, IntReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                    case 3:
                        if (ncountbytes == 1) {
                            std::cout << "&NewClusterTable<LongIntReader, LongIntReader, ByteReader>::s_getValue1AtRow";
                        } else {
                            std::cout << "&NewClusterTable<LongIntReader, LongIntReader, IntReader>::s_getValue1AtRow";
                        }
                        break;
                }
                break;
        }
    }

    static void getReader(const char nbytes1,
            const char nbytes2,
            const char ncountbytes,
            long (**output)(const char*, const long)) {

        switch (nbytes1) {
            case 0:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<ByteReader, ByteReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<ByteReader, ByteReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 1:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<ByteReader, ShortReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<ByteReader, ShortReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 2:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<ByteReader, IntReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<ByteReader, IntReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 3:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<ByteReader, LongIntReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<ByteReader, LongIntReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                }
                break;
            case 1:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<ShortReader, ByteReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<ShortReader, ByteReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 1:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<ShortReader, ShortReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<ShortReader, ShortReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 2:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<ShortReader, IntReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<ShortReader, IntReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 3:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<ShortReader, LongIntReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<ShortReader, LongIntReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                }
                break;
            case 2:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<IntReader, ByteReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<IntReader, ByteReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 1:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<IntReader, ShortReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<IntReader, ShortReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 2:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<IntReader, IntReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<IntReader, IntReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 3:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<IntReader, LongIntReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<IntReader, LongIntReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                }
                break;
            case 3:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<LongIntReader, ByteReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<LongIntReader, ByteReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 1:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<LongIntReader, ShortReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<LongIntReader, ShortReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 2:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<LongIntReader, IntReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<LongIntReader, IntReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                    case 3:
                        if (ncountbytes == 1) {
                            *output = &NewClusterTable<LongIntReader, LongIntReader, ByteReader>::s_getValue1AtRow;
                        } else {
                            *output = &NewClusterTable<LongIntReader, LongIntReader, IntReader>::s_getValue1AtRow;
                        }
                        break;
                }
                break;
        }
    }

    PairItr *get(const char nbytes1, const char nbytes2,
            const char ncountbytes) {
        switch (nbytes1) {
            case 0:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            return f111.get();
                        } else {
                            return f112.get();
                        }
                    case 1:
                        if (ncountbytes == 1) {
                            return f121.get();
                        } else {
                            return f122.get();
                        }
                    case 2:
                        if (ncountbytes == 1) {
                            return f141.get();
                        } else {
                            return f142.get();
                        }
                    case 3:
                        if (ncountbytes == 1) {
                            return f181.get();
                        } else {
                            return f182.get();
                        }
                }
                break;
            case 1:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            return f211.get();
                        } else {
                            return f212.get();
                        }
                    case 1:
                        if (ncountbytes == 1) {
                            return f221.get();
                        } else {
                            return f222.get();
                        }
                    case 2:
                        if (ncountbytes == 1) {
                            return f241.get();
                        } else {
                            return f242.get();
                        }
                    case 3:
                        if (ncountbytes == 1) {
                            return f281.get();
                        } else {
                            return f282.get();
                        }
                }
                break;
            case 2:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            return f411.get();
                        } else {
                            return f412.get();
                        }
                    case 1:
                        if (ncountbytes == 1) {
                            return f421.get();
                        } else {
                            return f422.get();
                        }
                    case 2:
                        if (ncountbytes == 1) {
                            return f441.get();
                        } else {
                            return f442.get();
                        }
                    case 3:
                        if (ncountbytes == 1) {
                            return f481.get();
                        } else {
                            return f482.get();
                        }
                }
                break;
            case 3:
                switch (nbytes2) {
                    case 0:
                        if (ncountbytes == 1) {
                            return f811.get();
                        } else {
                            return f812.get();
                        }
                    case 1:
                        if (ncountbytes == 1) {
                            return f821.get();
                        } else {
                            return f822.get();
                        }
                    case 2:
                        if (ncountbytes == 1) {
                            return f841.get();
                        } else {
                            return f842.get();
                        }
                    case 3:
                        if (ncountbytes == 1) {
                            return f881.get();
                        } else {
                            return f882.get();
                        }
                }
                break;
        }
        throw 10;
    }

    void release(AbsNewTable *table) {
        const char rcs = table->getReaderCountSize();

        switch (table->getReaderSize1()) {
            case 1:
                switch (table->getReaderSize2()) {
                    case 1:
                        if (rcs == 1) {
                            f111.release((NewClusterTable<ByteReader, ByteReader, ByteReader> *)table);
                        } else {
                            f112.release((NewClusterTable<ByteReader, ByteReader, IntReader> *)table);
                        }
                        break;
                    case 2:
                        if (rcs == 1) {
                            f121.release((NewClusterTable<ByteReader, ShortReader, ByteReader> *)table);
                        } else {
                            f122.release((NewClusterTable<ByteReader, ShortReader, IntReader> *)table);
                        }
                        break;
                    case 4:
                        if (rcs == 1) {
                            f141.release((NewClusterTable<ByteReader, IntReader, ByteReader> *)table);
                        } else {
                            f142.release((NewClusterTable<ByteReader, IntReader, IntReader> *)table);
                        }
                        break;
                    case 5:
                        if (rcs == 1) {
                            f181.release((NewClusterTable<ByteReader, LongIntReader, ByteReader> *)table);
                        } else {
                            f182.release((NewClusterTable<ByteReader, LongIntReader, IntReader> *)table);
                        }
                        break;
                    default:
                        throw 10;
                }
                break;
            case 2:
                switch (table->getReaderSize2()) {
                    case 1:
                        if (rcs == 1) {
                            f211.release((NewClusterTable<ShortReader, ByteReader, ByteReader> *)table);
                        } else {
                            f212.release((NewClusterTable<ShortReader, ByteReader, IntReader> *)table);
                        }
                        break;
                    case 2:
                        if (rcs == 1) {
                            f221.release((NewClusterTable<ShortReader, ShortReader, ByteReader> *)table);
                        } else {
                            f222.release((NewClusterTable<ShortReader, ShortReader, IntReader> *)table);
                        }
                        break;
                    case 4:
                        if (rcs == 1) {
                            f241.release((NewClusterTable<ShortReader, IntReader, ByteReader> *)table);
                        } else {
                            f242.release((NewClusterTable<ShortReader, IntReader, IntReader> *)table);
                        }
                        break;
                    case 5:
                        if (rcs == 1) {
                            f281.release((NewClusterTable<ShortReader, LongIntReader, ByteReader> *)table);
                        } else {
                            f282.release((NewClusterTable<ShortReader, LongIntReader, IntReader> *)table);
                        }
                        break;
                    default:
                        throw 10;
                }
                break;
            case 4:
                switch (table->getReaderSize2()) {
                    case 1:
                        if (rcs == 1) {
                            f411.release((NewClusterTable<IntReader, ByteReader, ByteReader> *)table);
                        } else {
                            f412.release((NewClusterTable<IntReader, ByteReader, IntReader> *)table);
                        }
                        break;
                    case 2:
                        if (rcs == 1) {
                            f421.release((NewClusterTable<IntReader, ShortReader, ByteReader> *)table);
                        } else {
                            f422.release((NewClusterTable<IntReader, ShortReader, IntReader> *)table);
                        }
                        break;
                    case 4:
                        if (rcs == 1) {
                            f441.release((NewClusterTable<IntReader, IntReader, ByteReader> *)table);
                        } else {
                            f442.release((NewClusterTable<IntReader, IntReader, IntReader> *)table);
                        }
                        break;
                    case 5:
                        if (rcs == 1) {
                            f481.release((NewClusterTable<IntReader, LongIntReader, ByteReader> *)table);
                        } else {
                            f482.release((NewClusterTable<IntReader, LongIntReader, IntReader> *)table);
                        }
                        break;
                    default:
                        throw 10;
                }
                break;
            case 5:
                switch (table->getReaderSize2()) {
                    case 1:
                        if (rcs == 1) {
                            f811.release((NewClusterTable<LongIntReader, ByteReader, ByteReader> *)table);
                        } else {
                            f812.release((NewClusterTable<LongIntReader, ByteReader, IntReader> *)table);
                        }
                        break;
                    case 2:
                        if (rcs == 1) {
                            f821.release((NewClusterTable<LongIntReader, ShortReader, ByteReader> *)table);
                        } else {
                            f822.release((NewClusterTable<LongIntReader, ShortReader, IntReader> *)table);
                        }
                        break;
                    case 4:
                        if (rcs == 1) {
                            f841.release((NewClusterTable<LongIntReader, IntReader, ByteReader> *)table);
                        } else {
                            f842.release((NewClusterTable<LongIntReader, IntReader, IntReader> *)table);
                        }
                        break;
                    case 5:
                        if (rcs == 1) {
                            f881.release((NewClusterTable<LongIntReader, LongIntReader, ByteReader> *)table);
                        } else {
                            f882.release((NewClusterTable<LongIntReader, LongIntReader, IntReader> *)table);
                        }
                        break;
                    default:
                        throw 10;
                }
                break;
            default:
                throw 10;
        }
    }
};

struct FactoryNewRowTable {
    Factory<NewRowTable<ByteReader, ByteReader>> f11;
    Factory<NewRowTable<ByteReader, ShortReader>> f12;
    Factory<NewRowTable<ByteReader, IntReader>> f14;
    Factory<NewRowTable<ByteReader, LongIntReader>> f18;

    Factory<NewRowTable<ShortReader, ByteReader>> f21;
    Factory<NewRowTable<ShortReader, ShortReader>> f22;
    Factory<NewRowTable<ShortReader, IntReader>> f24;
    Factory<NewRowTable<ShortReader, LongIntReader>> f28;

    Factory<NewRowTable<IntReader, ByteReader>> f41;
    Factory<NewRowTable<IntReader, ShortReader>> f42;
    Factory<NewRowTable<IntReader, IntReader>> f44;
    Factory<NewRowTable<IntReader, LongIntReader>> f48;

    Factory<NewRowTable<LongIntReader, ByteReader>> f81;
    Factory<NewRowTable<LongIntReader, ShortReader>> f82;
    Factory<NewRowTable<LongIntReader, IntReader>> f84;
    Factory<NewRowTable<LongIntReader, LongIntReader>> f88;
    Factory<NewRowTable<LongIntReader, LongReader>> fspecial;

    static void getReaderName(const char nbytes1,
            const char nbytes2) {
        switch (nbytes1) {
            case 0:
                switch (nbytes2) {
                    case 0:
                        std::cout << "&NewRowTable<ByteReader, ByteReader>::s_getValue1AtRow";
                        break;
                    case 1:
                        std::cout << "&NewRowTable<ByteReader, ShortReader>::s_getValue1AtRow";
                        break;
                    case 2:
                        std::cout << "&NewRowTable<ByteReader, IntReader>::s_getValue1AtRow";
                        break;
                    case 3:
                        std::cout << "&NewRowTable<ByteReader, LongIntReader>::s_getValue1AtRow";
                        break;
                }
                break;
            case 1:
                switch (nbytes2) {
                    case 0:
                        std::cout << "&NewRowTable<ShortReader, ByteReader>::s_getValue1AtRow";
                        break;
                    case 1:
                        std::cout << "&NewRowTable<ShortReader, ShortReader>::s_getValue1AtRow";
                        break;
                    case 2:
                        std::cout << "&NewRowTable<ShortReader, IntReader>::s_getValue1AtRow";
                        break;
                    case 3:
                        std::cout << "&NewRowTable<ShortReader, LongIntReader>::s_getValue1AtRow";
                        break;
                }
                break;
            case 2:
                switch (nbytes2) {
                    case 0:
                        std::cout << "&NewRowTable<IntReader, ByteReader>::s_getValue1AtRow";
                        break;
                    case 1:
                        std::cout << "&NewRowTable<IntReader, ShortReader>::s_getValue1AtRow";
                        break;
                    case 2:
                        std::cout << "&NewRowTable<IntReader, IntReader>::s_getValue1AtRow";
                        break;
                    case 3:
                        std::cout << "&NewRowTable<IntReader, LongIntReader>::s_getValue1AtRow";
                        break;
                }
                break;
            case 3:
                switch (nbytes2) {
                    case 0:
                        std::cout << "&NewRowTable<LongIntReader, ByteReader>::s_getValue1AtRow";
                        break;
                    case 1:
                        std::cout << "&NewRowTable<LongIntReader, ShortReader>::s_getValue1AtRow";
                        break;
                    case 2:
                        std::cout << "&NewRowTable<LongIntReader, IntReader>::s_getValue1AtRow";
                        break;
                    case 3:
                        std::cout << "&NewRowTable<LongIntReader, LongIntReader>::s_getValue1AtRow";
                        break;
                    case 4:
                        std::cout << "&NewRowTable<LongIntReader, LongReader>::s_getValue1AtRow";
                        break;
                }
                break;
        }
    }
    static void getReader(const char nbytes1,
            const char nbytes2,
            long (**output)(const char*, const long)) {
        output = NULL;
        switch (nbytes1) {
            case 0:
                switch (nbytes2) {
                    case 0:
                        *output = &NewRowTable<ByteReader, ByteReader>::s_getValue1AtRow;
                        break;
                    case 1:
                        *output = &NewRowTable<ByteReader, ShortReader>::s_getValue1AtRow;
                        break;
                    case 2:
                        *output = &NewRowTable<ByteReader, IntReader>::s_getValue1AtRow;
                        break;
                    case 3:
                        *output = &NewRowTable<ByteReader, LongIntReader>::s_getValue1AtRow;
                        break;
                }
                break;
            case 1:
                switch (nbytes2) {
                    case 0:
                        *output = &NewRowTable<ShortReader, ByteReader>::s_getValue1AtRow;
                        break;
                    case 1:
                        *output = &NewRowTable<ShortReader, ShortReader>::s_getValue1AtRow;
                        break;
                    case 2:
                        *output = &NewRowTable<ShortReader, IntReader>::s_getValue1AtRow;
                        break;
                    case 3:
                        *output = &NewRowTable<ShortReader, LongIntReader>::s_getValue1AtRow;
                        break;
                }
                break;
            case 2:
                switch (nbytes2) {
                    case 0:
                        *output = &NewRowTable<IntReader, ByteReader>::s_getValue1AtRow;
                        break;
                    case 1:
                        *output = &NewRowTable<IntReader, ShortReader>::s_getValue1AtRow;
                        break;
                    case 2:
                        *output = &NewRowTable<IntReader, IntReader>::s_getValue1AtRow;
                        break;
                    case 3:
                        *output = &NewRowTable<IntReader, LongIntReader>::s_getValue1AtRow;
                        break;
                }
                break;
            case 3:
                switch (nbytes2) {
                    case 0:
                        *output = &NewRowTable<LongIntReader, ByteReader>::s_getValue1AtRow;
                        break;
                    case 1:
                        *output = &NewRowTable<LongIntReader, ShortReader>::s_getValue1AtRow;
                        break;
                    case 2:
                        *output = &NewRowTable<LongIntReader, IntReader>::s_getValue1AtRow;
                        break;
                    case 3:
                        *output = &NewRowTable<LongIntReader, LongIntReader>::s_getValue1AtRow;
                        break;
                    case 4:
                        *output = &NewRowTable<LongIntReader, LongReader>::s_getValue1AtRow;
                        break;
                }
                break;
        }
    }

    static void get12Reader(const char nbytes1,
            const char nbytes2,
            void (**output)(const uint64_t ,
                const uint8_t ,
                const char *,
                const uint64_t,
                uint64_t &,
                uint64_t &)) {

        switch (nbytes1) {
            case 0:
                switch (nbytes2) {
                    case 0:
                        *output = &NewRowTable<ByteReader, ByteReader>::s_getValue12AtRow;
                        break;
                    case 1:
                        *output = &NewRowTable<ByteReader, ShortReader>::s_getValue12AtRow;
                        break;
                    case 2:
                        *output = &NewRowTable<ByteReader, IntReader>::s_getValue12AtRow;
                        break;
                    case 3:
                        *output = &NewRowTable<ByteReader, LongIntReader>::s_getValue12AtRow;
                        break;
                }
                break;
            case 1:
                switch (nbytes2) {
                    case 0:
                        *output = &NewRowTable<ShortReader, ByteReader>::s_getValue12AtRow;
                        break;
                    case 1:
                        *output = &NewRowTable<ShortReader, ShortReader>::s_getValue12AtRow;
                        break;
                    case 2:
                        *output = &NewRowTable<ShortReader, IntReader>::s_getValue12AtRow;
                        break;
                    case 3:
                        *output = &NewRowTable<ShortReader, LongIntReader>::s_getValue12AtRow;
                        break;
                }
                break;
            case 2:
                switch (nbytes2) {
                    case 0:
                        *output = &NewRowTable<IntReader, ByteReader>::s_getValue12AtRow;
                        break;
                    case 1:
                        *output = &NewRowTable<IntReader, ShortReader>::s_getValue12AtRow;
                        break;
                    case 2:
                        *output = &NewRowTable<IntReader, IntReader>::s_getValue12AtRow;
                        break;
                    case 3:
                        *output = &NewRowTable<IntReader, LongIntReader>::s_getValue12AtRow;
                        break;
                }
                break;
            case 3:
                switch (nbytes2) {
                    case 0:
                        *output = &NewRowTable<LongIntReader, ByteReader>::s_getValue12AtRow;
                        break;
                    case 1:
                        *output = &NewRowTable<LongIntReader, ShortReader>::s_getValue12AtRow;
                        break;
                    case 2:
                        *output = &NewRowTable<LongIntReader, IntReader>::s_getValue12AtRow;
                        break;
                    case 3:
                        *output = &NewRowTable<LongIntReader, LongIntReader>::s_getValue12AtRow;
                        break;
                    case 4:
                        *output = &NewRowTable<LongIntReader, LongReader>::s_getValue12AtRow;
                        break;
                }
                break;
        }
    }


    PairItr *get(const char nbytes1, const char nbytes2) {
        switch (nbytes1) {
            case 0:
                switch (nbytes2) {
                    case 0:
                        return f11.get();
                    case 1:
                        return f12.get();
                    case 2:
                        return f14.get();
                    case 3:
                        return f18.get();
                }
                break;
            case 1:
                switch (nbytes2) {
                    case 0:
                        return f21.get();
                    case 1:
                        return f22.get();
                    case 2:
                        return f24.get();
                    case 3:
                        return f28.get();
                }
                break;
            case 2:
                switch (nbytes2) {
                    case 0:
                        return f41.get();
                    case 1:
                        return f42.get();
                    case 2:
                        return f44.get();
                    case 3:
                        return f48.get();
                }
                break;
            case 3:
                switch (nbytes2) {
                    case 0:
                        return f81.get();
                    case 1:
                        return f82.get();
                    case 2:
                        return f84.get();
                    case 3:
                        return f88.get();
                    case 4:
                        return fspecial.get();
                }
                break;
        }
        throw 10;
    }

    void release(AbsNewTable *table) {
        switch (table->getReaderSize1()) {
            case 1:
                switch (table->getReaderSize2()) {
                    case 1:
                        f11.release((NewRowTable<ByteReader, ByteReader> *)table);
                        break;
                    case 2:
                        f12.release((NewRowTable<ByteReader, ShortReader> *)table);
                        break;
                    case 4:
                        f14.release((NewRowTable<ByteReader, IntReader> *)table);
                        break;
                    case 5:
                        f18.release((NewRowTable<ByteReader, LongIntReader> *)table);
                        break;
                    default:
                        throw 10;
                }
                break;
            case 2:
                switch (table->getReaderSize2()) {
                    case 1:
                        f21.release((NewRowTable<ShortReader, ByteReader> *)table);
                        break;
                    case 2:
                        f22.release((NewRowTable<ShortReader, ShortReader> *)table);
                        break;
                    case 4:
                        f24.release((NewRowTable<ShortReader, IntReader> *)table);
                        break;
                    case 5:
                        f28.release((NewRowTable<ShortReader, LongIntReader> *)table);
                        break;
                    default:
                        throw 10;
                }
                break;
            case 4:
                switch (table->getReaderSize2()) {
                    case 1:
                        f41.release((NewRowTable<IntReader, ByteReader> *)table);
                        break;
                    case 2:
                        f42.release((NewRowTable<IntReader, ShortReader> *)table);
                        break;
                    case 4:
                        f44.release((NewRowTable<IntReader, IntReader> *)table);
                        break;
                    case 5:
                        f48.release((NewRowTable<IntReader, LongIntReader> *)table);
                        break;
                    default:
                        throw 10;
                }
                break;
            case 5:
                switch (table->getReaderSize2()) {
                    case 1:
                        f81.release((NewRowTable<LongIntReader, ByteReader> *)table);
                        break;
                    case 2:
                        f82.release((NewRowTable<LongIntReader, ShortReader> *)table);
                        break;
                    case 4:
                        f84.release((NewRowTable<LongIntReader, IntReader> *)table);
                        break;
                    case 5:
                        f88.release((NewRowTable<LongIntReader, LongIntReader> *)table);
                        break;
                    case 8:
                        fspecial.release((NewRowTable<LongIntReader, LongReader> *)table);
                        break;
                    default:
                        throw 10;
                }
                break;
            default:
                throw 10;
        }
    }
};

#endif

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


#include<snap/directed.h>

Trident_TNGraph::Trident_TNGraph(KB *kb) {
    this->kb = kb;
    this->q = kb->query();
    SnapReaders::loadAllFiles(kb);
    string path = kb->getPath() + string("/tree/flat");
    mapping = bip::file_mapping(path.c_str(), bip::read_only);
    mapped_rgn = bip::mapped_region(mapping, bip::read_only);
    rawnodes = static_cast<char*>(mapped_rgn.get_address());
    nnodes = (uint64_t)mapped_rgn.get_size() / 31;
}

bool Trident_TNGraph::getNodeInfo(TermCoordinates &coord,
        Querier *q,
        long &indeg,
        long &outdeg,
        const char*& osp,
        const char*& sop,
        SnapReaders::pReader& ospReader,
        SnapReaders::pReader& sopReader) {

    bool ok = false;
    if (coord.exists(IDX_OSP)) {
        indeg = coord.getNElements(IDX_OSP);
        const short fileIn = coord.getFileIdx(IDX_OSP);
        const int markIn = coord.getMark(IDX_OSP);
        osp = q->getTable(IDX_OSP, fileIn, markIn);
        const char strategy = coord.getStrategy(IDX_OSP);
        const int storageType = StorageStrat::getStorageType(strategy);
        if (storageType == NEWCOLUMN_ITR) {
            const uint8_t header1 = (uint8_t) osp[0];
            const uint8_t bytesPerFirstEntry = (header1 >> 3) & 7;
            const uint8_t header2 = (uint8_t) osp[1];
            const uint8_t bytesPerStartingPoint =  header2 & 7;
            const uint8_t bytesPerCount = (header2 >> 3) & 7;
            const uint8_t bytesFirstBlock = bytesPerFirstEntry + bytesPerCount + bytesPerStartingPoint;
            //Move osp to the beginning of the location
            int offset = 2;
            Utils::decode_vlong2(osp, &offset); //unused
            Utils::decode_vlong2(osp, &offset); //unused
            osp += offset;
            getNewColumnPointer(bytesPerFirstEntry, bytesFirstBlock, &ospReader);
        } else if (storageType == NEWROW_ITR) {
            const char nbytes1 = (strategy >> 3) & 3;
            const char nbytes2 = (strategy >> 1) & 3;
            FactoryNewRowTable::getReader(nbytes1, nbytes2, &ospReader);
        } else {
            const char nbytes1 = (strategy >> 3) & 3;
            const char nbytes2 = (strategy >> 1) & 3;
            char ncount = 1;
            if (strategy & 1)
                ncount = 4;
            FactoryNewClusterTable::getReader(nbytes1, nbytes2, ncount, &ospReader);
        }
        ok = true;
    }

    if (coord.exists(IDX_SOP)) {
        outdeg = coord.getNElements(IDX_SOP);
        const short fileOut = coord.getFileIdx(IDX_SOP);
        const int markOut = coord.getMark(IDX_SOP);
        sop = q->getTable(IDX_SOP, fileOut, markOut);
        const char strategy = coord.getStrategy(IDX_SOP);
        const int storageType = StorageStrat::getStorageType(strategy);
        if (storageType == NEWCOLUMN_ITR) {
            const uint8_t header1 = (uint8_t) sop[0];
            const uint8_t bytesPerFirstEntry = (header1 >> 3) & 7;
            const uint8_t header2 = (uint8_t) sop[1];
            const uint8_t bytesPerStartingPoint =  header2 & 7;
            const uint8_t bytesPerCount = (header2 >> 3) & 7;
            const uint8_t bytesFirstBlock = bytesPerFirstEntry + bytesPerCount + bytesPerStartingPoint;
            //Move sop to the beginning of the location
            int offset = 2;
            Utils::decode_vlong2(sop, &offset); //unused
            Utils::decode_vlong2(sop, &offset); //unused
            sop += offset;
            getNewColumnPointer(bytesPerFirstEntry, bytesFirstBlock, &sopReader);
        } else if (storageType == NEWROW_ITR) {
            const char nbytes1 = (strategy >> 3) & 3;
            const char nbytes2 = (strategy >> 1) & 3;
            FactoryNewRowTable::getReader(nbytes1, nbytes2, &sopReader);
        } else {
            const char nbytes1 = (strategy >> 3) & 3;
            const char nbytes2 = (strategy >> 1) & 3;
            char ncount = 1;
            if (strategy & 1)
                ncount = 4;
            FactoryNewClusterTable::getReader(nbytes1, nbytes2, ncount, &sopReader);
        }
        ok = true;
    }
    if (!ok) {
        BOOST_LOG_TRIVIAL(error) << "One node is skipped";
        throw 10;
    }
    return ok;
}

void Trident_TNGraph::getNewColumnPointer(int bytesEl,
        int totalBytes,
        long (**output)(const char*, const long)) {
    int remBytes = totalBytes - bytesEl;
    switch (bytesEl) {
        case 1:
            switch (remBytes) {
                case 2:
                    *output = &NewColumnTable::s_getValue1AtRow<1,2>; break;
                case 3:
                    *output = &NewColumnTable::s_getValue1AtRow<1,3>; break;
                case 4:
                    *output = &NewColumnTable::s_getValue1AtRow<1,4>; break;
                case 5:
                    *output = &NewColumnTable::s_getValue1AtRow<1,5>; break;
                case 6:
                    *output = &NewColumnTable::s_getValue1AtRow<1,6>; break;
                case 7:
                    *output = &NewColumnTable::s_getValue1AtRow<1,7>; break;
                case 8:
                    *output = &NewColumnTable::s_getValue1AtRow<1,8>; break;
                case 9:
                    *output = &NewColumnTable::s_getValue1AtRow<1,9>; break;
                case 10:
                    *output = &NewColumnTable::s_getValue1AtRow<1,10>; break;
                case 11:
                    *output = &NewColumnTable::s_getValue1AtRow<1,11>; break;
                case 12:
                    *output = &NewColumnTable::s_getValue1AtRow<1,12>; break;
                case 13:
                    *output = &NewColumnTable::s_getValue1AtRow<1,13>; break;
                case 14:
                    *output = &NewColumnTable::s_getValue1AtRow<1,14>; break;
                case 15:
                    *output = &NewColumnTable::s_getValue1AtRow<1,15>; break;
                case 16:
                    *output = &NewColumnTable::s_getValue1AtRow<1,16>; break;
                default:
                    throw 10;
            }
            break;
        case 2:
            switch (remBytes) {
                case 2:
                    *output = &NewColumnTable::s_getValue1AtRow<2,2>; break;
                case 3:
                    *output = &NewColumnTable::s_getValue1AtRow<2,3>; break;
                case 4:
                    *output = &NewColumnTable::s_getValue1AtRow<2,4>; break;
                case 5:
                    *output = &NewColumnTable::s_getValue1AtRow<2,5>; break;
                case 6:
                    *output = &NewColumnTable::s_getValue1AtRow<2,6>; break;
                case 7:
                    *output = &NewColumnTable::s_getValue1AtRow<2,7>; break;
                case 8:
                    *output = &NewColumnTable::s_getValue1AtRow<2,8>; break;
                case 9:
                    *output = &NewColumnTable::s_getValue1AtRow<2,9>; break;
                case 10:
                    *output = &NewColumnTable::s_getValue1AtRow<2,10>; break;
                case 11:
                    *output = &NewColumnTable::s_getValue1AtRow<2,11>; break;
                case 12:
                    *output = &NewColumnTable::s_getValue1AtRow<2,12>; break;
                case 13:
                    *output = &NewColumnTable::s_getValue1AtRow<2,13>; break;
                case 14:
                    *output = &NewColumnTable::s_getValue1AtRow<2,14>; break;
                case 15:
                    *output = &NewColumnTable::s_getValue1AtRow<2,15>; break;
                case 16:
                    *output = &NewColumnTable::s_getValue1AtRow<2,16>; break;
                default:
                    throw 10;
            }
            break;
        case 3:
            switch (remBytes) {
                case 2:
                    *output = &NewColumnTable::s_getValue1AtRow<3,2>; break;
                case 3:
                    *output = &NewColumnTable::s_getValue1AtRow<3,3>; break;
                case 4:
                    *output = &NewColumnTable::s_getValue1AtRow<3,4>; break;
                case 5:
                    *output = &NewColumnTable::s_getValue1AtRow<3,5>; break;
                case 6:
                    *output = &NewColumnTable::s_getValue1AtRow<3,6>; break;
                case 7:
                    *output = &NewColumnTable::s_getValue1AtRow<3,7>; break;
                case 8:
                    *output = &NewColumnTable::s_getValue1AtRow<3,8>; break;
                case 9:
                    *output = &NewColumnTable::s_getValue1AtRow<3,9>; break;
                case 10:
                    *output = &NewColumnTable::s_getValue1AtRow<3,10>; break;
                case 11:
                    *output = &NewColumnTable::s_getValue1AtRow<3,11>; break;
                case 12:
                    *output = &NewColumnTable::s_getValue1AtRow<3,12>; break;
                case 13:
                    *output = &NewColumnTable::s_getValue1AtRow<3,13>; break;
                case 14:
                    *output = &NewColumnTable::s_getValue1AtRow<3,14>; break;
                case 15:
                    *output = &NewColumnTable::s_getValue1AtRow<3,15>; break;
                case 16:
                    *output = &NewColumnTable::s_getValue1AtRow<3,16>; break;
                default:
                    throw 10;
            }
            break;
        case 4:
            switch (remBytes) {
                case 2:
                    *output = &NewColumnTable::s_getValue1AtRow<4,2>; break;
                case 3:
                    *output = &NewColumnTable::s_getValue1AtRow<4,3>; break;
                case 4:
                    *output = &NewColumnTable::s_getValue1AtRow<4,4>; break;
                case 5:
                    *output = &NewColumnTable::s_getValue1AtRow<4,5>; break;
                case 6:
                    *output = &NewColumnTable::s_getValue1AtRow<4,6>; break;
                case 7:
                    *output = &NewColumnTable::s_getValue1AtRow<4,7>; break;
                case 8:
                    *output = &NewColumnTable::s_getValue1AtRow<4,8>; break;
                case 9:
                    *output = &NewColumnTable::s_getValue1AtRow<4,9>; break;
                case 10:
                    *output = &NewColumnTable::s_getValue1AtRow<4,10>; break;
                case 11:
                    *output = &NewColumnTable::s_getValue1AtRow<4,11>; break;
                case 12:
                    *output = &NewColumnTable::s_getValue1AtRow<4,12>; break;
                case 13:
                    *output = &NewColumnTable::s_getValue1AtRow<4,13>; break;
                case 14:
                    *output = &NewColumnTable::s_getValue1AtRow<4,14>; break;
                case 15:
                    *output = &NewColumnTable::s_getValue1AtRow<4,15>; break;
                case 16:
                    *output = &NewColumnTable::s_getValue1AtRow<4,16>; break;
                default:
                    throw 10;
            }
            break;
        case 5:
            switch (remBytes) {
                case 2:
                    *output = &NewColumnTable::s_getValue1AtRow<5,2>; break;
                case 3:
                    *output = &NewColumnTable::s_getValue1AtRow<5,3>; break;
                case 4:
                    *output = &NewColumnTable::s_getValue1AtRow<5,4>; break;
                case 5:
                    *output = &NewColumnTable::s_getValue1AtRow<5,5>; break;
                case 6:
                    *output = &NewColumnTable::s_getValue1AtRow<5,6>; break;
                case 7:
                    *output = &NewColumnTable::s_getValue1AtRow<5,7>; break;
                case 8:
                    *output = &NewColumnTable::s_getValue1AtRow<5,8>; break;
                case 9:
                    *output = &NewColumnTable::s_getValue1AtRow<5,9>; break;
                case 10:
                    *output = &NewColumnTable::s_getValue1AtRow<5,10>; break;
                case 11:
                    *output = &NewColumnTable::s_getValue1AtRow<5,11>; break;
                case 12:
                    *output = &NewColumnTable::s_getValue1AtRow<5,12>; break;
                case 13:
                    *output = &NewColumnTable::s_getValue1AtRow<5,13>; break;
                case 14:
                    *output = &NewColumnTable::s_getValue1AtRow<5,14>; break;
                case 15:
                    *output = &NewColumnTable::s_getValue1AtRow<5,15>; break;
                case 16:
                    *output = &NewColumnTable::s_getValue1AtRow<5,16>; break;
                default:
                    throw 10;
            }
            break;
        default:
            throw 10;
    }
}

void Trident_TNGraph::getNewColumnPointer_name(int bytesEl, int totalBytes) {
    int remBytes = totalBytes - bytesEl;
    switch (bytesEl) {
        case 1:
            switch (remBytes) {
                case 2:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,2>," << std::endl; break;
                case 3:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,3>," << std::endl; break;
                case 4:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,4>," << std::endl; break;
                case 5:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,5>," << std::endl; break;
                case 6:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,6>," << std::endl; break;
                case 7:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,7>," << std::endl; break;
                case 8:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,8>," << std::endl; break;
                case 9:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,9>," << std::endl; break;
                case 10:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,10>," << std::endl; break;
                case 11:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,11>," << std::endl; break;
                case 12:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,12>," << std::endl; break;
                case 13:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,13>," << std::endl; break;
                case 14:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,14>," << std::endl; break;
                case 15:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,15>," << std::endl; break;
                case 16:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<1,16>," << std::endl; break;
                default:
                    throw 10;
            }
            break;
        case 2:
            switch (remBytes) {
                case 2:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,2>," << std::endl; break;
                case 3:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,3>," << std::endl; break;
                case 4:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,4>," << std::endl; break;
                case 5:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,5>," << std::endl; break;
                case 6:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,6>," << std::endl; break;
                case 7:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,7>," << std::endl; break;
                case 8:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,8>," << std::endl; break;
                case 9:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,9>," << std::endl; break;
                case 10:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,10>," << std::endl; break;
                case 11:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,11>," << std::endl; break;
                case 12:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,12>," << std::endl; break;
                case 13:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,13>," << std::endl; break;
                case 14:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,14>," << std::endl; break;
                case 15:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,15>," << std::endl; break;
                case 16:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<2,16>," << std::endl; break;
                default:
                    throw 10;
            }
            break;
        case 3:
            switch (remBytes) {
                case 2:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,2>," << std::endl; break;
                case 3:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,3>," << std::endl; break;
                case 4:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,4>," << std::endl; break;
                case 5:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,5>," << std::endl; break;
                case 6:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,6>," << std::endl; break;
                case 7:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,7>," << std::endl; break;
                case 8:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,8>," << std::endl; break;
                case 9:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,9>," << std::endl; break;
                case 10:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,10>," << std::endl; break;
                case 11:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,11>," << std::endl; break;
                case 12:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,12>," << std::endl; break;
                case 13:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,13>," << std::endl; break;
                case 14:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,14>," << std::endl; break;
                case 15:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,15>," << std::endl; break;
                case 16:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<3,16>," << std::endl; break;
                default:
                    throw 10;
            }
            break;
        case 4:
            switch (remBytes) {
                case 2:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,2>," << std::endl; break;
                case 3:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,3>," << std::endl; break;
                case 4:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,4>," << std::endl; break;
                case 5:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,5>," << std::endl; break;
                case 6:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,6>," << std::endl; break;
                case 7:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,7>," << std::endl; break;
                case 8:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,8>," << std::endl; break;
                case 9:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,9>," << std::endl; break;
                case 10:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,10>," << std::endl; break;
                case 11:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,11>," << std::endl; break;
                case 12:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,12>," << std::endl; break;
                case 13:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,13>," << std::endl; break;
                case 14:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,14>," << std::endl; break;
                case 15:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,15>," << std::endl; break;
                case 16:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<4,16>," << std::endl; break;
                default:
                    throw 10;
            }
            break;
        case 5:
            switch (remBytes) {
                case 2:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,2>," << std::endl; break;
                case 3:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,3>," << std::endl; break;
                case 4:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,4>," << std::endl; break;
                case 5:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,5>," << std::endl; break;
                case 6:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,6>," << std::endl; break;
                case 7:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,7>," << std::endl; break;
                case 8:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,8>," << std::endl; break;
                case 9:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,9>," << std::endl; break;
                case 10:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,10>," << std::endl; break;
                case 11:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,11>," << std::endl; break;
                case 12:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,12>," << std::endl; break;
                case 13:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,13>," << std::endl; break;
                case 14:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,14>," << std::endl; break;
                case 15:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,15>," << std::endl; break;
                case 16:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<5,16>," << std::endl; break;
                default:
                    throw 10;
            }
            break;
        case 6:
            switch (remBytes) {
                case 2:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,2>," << std::endl; break;
                case 3:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,3>," << std::endl; break;
                case 4:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,4>," << std::endl; break;
                case 5:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,5>," << std::endl; break;
                case 6:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,6>," << std::endl; break;
                case 7:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,7>," << std::endl; break;
                case 8:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,8>," << std::endl; break;
                case 9:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,9>," << std::endl; break;
                case 10:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,10>," << std::endl; break;
                case 11:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,11>," << std::endl; break;
                case 12:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,12>," << std::endl; break;
                case 13:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,13>," << std::endl; break;
                case 14:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,14>," << std::endl; break;
                case 15:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,15>," << std::endl; break;
                case 16:
                    std::cout << "&NewColumnTable::s_getValue1AtRow<6,16>," << std::endl; break;
                default:
                    throw 10;
            }
            break;

        default:
            throw 10;
    }
}

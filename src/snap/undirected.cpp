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


#include <snap/directed.h>
#include <snap/undirected.h>

Trident_UTNGraph::Trident_UTNGraph(KB *kb) {
    this->kb = kb;
    this->q = kb->query();
    SnapReaders::loadAllFiles(kb);
    string path = kb->getPath() + string("/tree/flat");
    mapping = bip::file_mapping(path.c_str(), bip::read_only);
    mapped_rgn = bip::mapped_region(mapping, bip::read_only);
    rawnodes = static_cast<char*>(mapped_rgn.get_address());
    nnodes = (uint64_t)mapped_rgn.get_size() / 31;
}

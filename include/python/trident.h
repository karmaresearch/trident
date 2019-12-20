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


#ifndef _PYTHON_H
#define _PYTHON_H

//#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <Python.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/ml/batch.h>

#include <layers/TridentLayer.hpp>

typedef struct {
    PyObject_HEAD
        KB *kb = NULL;
    Querier *q = NULL;
    std::unique_ptr<TridentLayer> db;
    bool rmKbOnDelete = false;
} trident_Db;

typedef struct {
    PyObject_HEAD
        Querier *q;
    PairItr *itr;
    char pos;
} trident_Itr;

extern PyTypeObject trident_ItrType;
extern PyTypeObject trident_DbType;

typedef struct {
    PyObject_HEAD
        std::unique_ptr<BatchCreator> creator;
    std::vector<uint64_t> batch1;
    std::vector<uint64_t> batch2;
    std::vector<uint64_t> batch3;
    int64_t batchsize;
} trident_Batcher;

extern PyTypeObject trident_BatcherType;

PyMODINIT_FUNC PyInit_analytics(void);

#endif

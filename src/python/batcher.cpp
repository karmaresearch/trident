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


#include <Python.h>
#include <python/trident.h>

#include <numpy/arrayobject.h>

static PyObject *Batcher_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    trident_Batcher *self;
    self = (trident_Batcher*)type->tp_alloc(type, 0);
    return (PyObject *)self;
}

static int Batcher_init(trident_Batcher *self, PyObject *args, PyObject *kwds) {
    const char *path;
    long batchsize, nthreads;
    if (!PyArg_ParseTuple(args, "sll", &path, &batchsize, &nthreads))
        return -1;
    self->creator = std::unique_ptr<BatchCreator>(new BatchCreator(string(path), batchsize, nthreads));
    self->batch.resize(batchsize*3);
    self->batchsize = batchsize;
    import_array();
    return 0;
}

static void Batcher_dealloc(trident_Itr* self) {
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *batcher_start(PyObject *self, PyObject *args) {
    trident_Batcher *s = (trident_Batcher*) self;
    s->creator->start();
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *batcher_getbatch(PyObject *self, PyObject *args) {
    trident_Batcher *s = (trident_Batcher*) self;
    bool out = s->creator->getBatch(s->batch);
    if (out) {
        int size = s->batch.size();
        npy_intp dims[2] = { size / 3 , 3 };
        PyObject* numpyArray = PyArray_SimpleNewFromData(2, dims, NPY_UINT64, (void*)s->batch.data());
        return numpyArray;
    } else {
        Py_INCREF(Py_None);
        return Py_None;
    }
}

static PyMethodDef Batcher_methods[] = {
    {"start", batcher_start, METH_VARARGS, "Starts the process." },
    {"getbatch", batcher_getbatch, METH_VARARGS, "Get one batch." },
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyTypeObject trident_BatcherType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        "trident.Batcher",             /* tp_name */
    sizeof(trident_Batcher),       /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Batcher_dealloc,   /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT |
        Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "Trident Batcher",        /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    Batcher_methods,                /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Batcher_init,        /* tp_init */
    0,                         /* tp_alloc */
    Batcher_new,                   /* tp_new */
};

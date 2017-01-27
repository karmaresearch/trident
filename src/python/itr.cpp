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
#include <iostream>
#include <vector>

#include <python/trident.h>

#include <boost/log/trivial.hpp>

static PyObject *Itr_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    trident_Itr *self;
    self = (trident_Itr*)type->tp_alloc(type, 0);
    self->q = NULL;
    self->itr = NULL;
    return (PyObject *)self;
}

static int Itr_init(trident_Itr *self, PyObject *args, PyObject *kwds) {
    self->q = NULL;
    self->itr = NULL;
    return 0;
}

static void Itr_dealloc(trident_Itr* self) {
    if (self->q != NULL) {
        if (self->itr != NULL) {
            self->q->releaseItr(self->itr);
        }
    }
    self->q = NULL;
    self->itr = NULL;
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *itr_field1(PyObject *self, PyObject *args) {
    trident_Itr *s = (trident_Itr*) self;
    if (s->itr == NULL) {
        return PyLong_FromLong(-1);
    } else {
        if (s->itr->hasNext()) {
            s->itr->next();
            switch (s->pos) {
                case 0:
                    return PyLong_FromLong(s->itr->getKey());
                case 1:
                    return PyLong_FromLong(s->itr->getValue1());
                case 2:
                    return PyLong_FromLong(s->itr->getValue2());
                default:
                    return PyLong_FromLong(-1);
            }
        } else {
            s->q->releaseItr(s->itr);
            s->q = NULL;
            s->itr = NULL;
            return PyLong_FromLong(-1);
        }
    }
}

static PyMethodDef Itr_methods[] = {
    //TODO
    {"field1", itr_field1, METH_VARARGS, "Returns the content of the first column and move to the next one. If there is no content the method returns -1." },
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyTypeObject trident_ItrType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        "trident.Itr",             /* tp_name */
    sizeof(trident_Itr),       /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Itr_dealloc,   /* tp_dealloc */
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
    "Trident Iterator",        /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    Itr_methods,                /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Itr_init,        /* tp_init */
    0,                         /* tp_alloc */
    Itr_new,                   /* tp_new */
};

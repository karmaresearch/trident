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

#include <boost/log/trivial.hpp>

#include <numpy/arrayobject.h>

#include <python/trident.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/tree/stringbuffer.h>

using namespace std;
namespace logging = boost::log;

typedef struct {
    PyObject_HEAD
        KB *kb;
    Querier *q;
} trident_Db;

static PyObject * Db_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    trident_Db *self;
    self = (trident_Db*)type->tp_alloc(type, 0);
    self->kb = NULL;
    return (PyObject *)self;
}

static int Db_init(trident_Db *self, PyObject *args, PyObject *kwds) {
    const char *path;
    if (!PyArg_ParseTuple(args, "s", &path))
        return -1;

    // Create a new trident database and return and ID to it
    KBConfig config;
    std::vector<string> locUpdates;
    self->kb = new KB(path, true, false, true, config, locUpdates);
    self->q = self->kb->query();
    return 0;
}

static PyObject *db_exists(PyObject *self, PyObject *args) {
    long s, p, o;
    if (!PyArg_ParseTuple(args, "lll", &s, &p, &o))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    const long nresults = q->getCardOnIndex(IDX_SPO, s, p, o);
    return PyBool_FromLong(nresults);
}

static PyObject *db_existsQuery(PyObject *self, PyObject *args) {
    long term;
    PyObject *tuple;
    const char *pattern;
    if (!PyArg_ParseTuple(args, "lOs", &term, &tuple, &pattern))
        return NULL;

    if (strcmp(pattern, "?cxxcc") == 0) {
        PyObject *op1 = PyTuple_GetItem(tuple, 0);
        PyObject *op2 = PyTuple_GetItem(tuple, 1);
        PyObject *oo2 = PyTuple_GetItem(tuple, 2);
        long p1 = PyLong_AsLong(op1);
        long p2 = PyLong_AsLong(op2);
        long o2 = PyLong_AsLong(oo2);
        Querier *q = ((trident_Db*)self)->q;
        auto itr1 = q->getPermuted(IDX_SPO, term, p1, -1, true);
        auto itr2 = q->getPermuted(IDX_OPS, o2, p2, -1, true);
        //Merge join
        long found = 0;
        long v2 = -1;
        while (itr1->hasNext()) {
            itr1->next();
            long v1 = itr1->getValue2();
            while (v2 == -1 || v2 < v1) {
                if (itr2->hasNext()) {
                    itr2->next();
                    v2 = itr2->getValue2();
                } else {
                    v2 = -1;
                    break;
                }
            }
            if (v2 != -1) {
                if (v2 == v1) {
                    found = 1;
                    break;
                }
            } else {
                //Second iterator is finished!
                break;
            }
        }

        q->releaseItr(itr1);
        q->releaseItr(itr2);
        return PyBool_FromLong(found);
    } else {
        cerr << "Not yet implemented" << endl;
        return PyBool_FromLong(0);
    }
}

static PyObject *db_counts(PyObject *self, PyObject *args) {
    long s;
    if (!PyArg_ParseTuple(args, "l", &s))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    const long nresults = q->getCardOnIndex(IDX_SPO, s, -1, -1);
    return PyLong_FromLong(nresults);
}

static PyObject *db_counto(PyObject *self, PyObject *args) {
    long o;
    if (!PyArg_ParseTuple(args, "l", &o))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    const long nresults = q->getCardOnIndex(IDX_OPS, -1, -1, o);
    return PyLong_FromLong(nresults);
}

static PyObject *db_countp(PyObject *self, PyObject *args) {
    long p;
    if (!PyArg_ParseTuple(args, "l", &p))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    const long nresults = q->getCardOnIndex(IDX_POS, -1, p, -1);
    return PyLong_FromLong(nresults);
}

static PyObject *db_ns(PyObject *self, PyObject *args) {
    long p, o;
    if (!PyArg_ParseTuple(args, "ll", &p, &o))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    const long nresults = q->getCardOnIndex(IDX_OPS, -1, p, o);
    return PyLong_FromLong(nresults);
}

static PyObject *db_no(PyObject *self, PyObject *args) {
    long s, p;
    if (!PyArg_ParseTuple(args, "ll", &s, &p))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    const long nresults = q->getCardOnIndex(IDX_SPO, s, p, -1);
    return PyLong_FromLong(nresults);
}

static PyObject *db_alls(PyObject *self, PyObject *args) {
    long p, o;
    if (!PyArg_ParseTuple(args, "ll", &p, &o))
        return NULL;

    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);

    PairItr *itr = q->getPermuted(IDX_OPS, o, p, -1, true);
    while (itr->hasNext()) {
        itr->next();
        long s = itr->getValue2();
        PyObject *value = PyLong_FromLong(s);
        PyList_Append(obj, value);
        Py_DECREF(value);
    }
    q->releaseItr(itr);
    return obj;
}

static PyObject *db_alls_fast(PyObject *self, PyObject *args) {
    long p, o;
    if (!PyArg_ParseTuple(args, "ll", &p, &o))
        return NULL;

    trident_Itr *obj = PyObject_New(trident_Itr, &trident_ItrType);
    Querier *q = ((trident_Db*)self)->q;
    PairItr *itr = q->getPermuted(IDX_OPS, o, p, -1, true);
    obj->q = q;
    obj->itr = itr;
    obj->pos = 2;
    return (PyObject*) obj;
}

static PyObject *db_alls_aggr(PyObject *self, PyObject *args) {
    long o;
    if (!PyArg_ParseTuple(args, "l", &o))
        return NULL;

    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);

    PairItr *itr = q->getPermuted(IDX_OSP, o, -1, -1, true);
    itr->ignoreSecondColumn();
    while (itr->hasNext()) {
        itr->next();
        long s = itr->getValue1();
        PyObject *value = PyLong_FromLong(s);
        PyList_Append(obj, value);
        Py_DECREF(value);
    }
    q->releaseItr(itr);
    return obj;
}

static PyObject *db_lists(PyObject *self, PyObject *args) {
    int text = 0;
    if (!PyArg_ParseTuple(args, "|b", &text))
        return NULL;
    char term[MAX_TERM_SIZE];
    DictMgmt *dict = ((trident_Db*)self)->kb->getDictMgmt();

    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);
    PairItr *itr = q->getTermList(IDX_SPO);
    while (itr->hasNext()) {
        itr->next();
        long s = itr->getKey();
        PyObject *value;
        if (!text) {
            value = PyLong_FromLong(s);
        } else {
            int len;
            bool resp = dict->getText(s, term, len);
            if (resp) {
                value = PyUnicode_FromStringAndSize(term, len);
            } else {
                value = PyUnicode_FromString("None");
            }
        }
        PyList_Append(obj, value);
        Py_DECREF(value);
    }
    q->releaseItr(itr);
    return obj;
}

static PyObject *db_listp(PyObject *self, PyObject *args) {
    int text = 0;
    if (!PyArg_ParseTuple(args, "|b", &text))
        return NULL;
    char term[MAX_TERM_SIZE];
    DictMgmt *dict =  ((trident_Db*)self)->kb->getDictMgmt();

    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);
    PairItr *itr = q->getTermList(IDX_POS);
    while (itr->hasNext()) {
        itr->next();
        long s = itr->getKey();
        PyObject *value;
        if (!text) {
            value = PyLong_FromLong(s);
        } else {
            int len;
            bool resp = dict->getText(s, term, len);
            if (resp) {
                value = PyUnicode_FromStringAndSize(term, len);
            } else {
                value = PyUnicode_FromString("None");
            }
        }
        PyList_Append(obj, value);
        Py_DECREF(value);
    }
    q->releaseItr(itr);
    return obj;
}

static PyObject *db_listo(PyObject *self, PyObject *args) {
    int text = 0;
    if (!PyArg_ParseTuple(args, "|b", &text))
        return NULL;
    char term[MAX_TERM_SIZE];
    DictMgmt *dict = ((trident_Db*)self)->kb->getDictMgmt();

    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);
    PairItr *itr = q->getTermList(IDX_OPS);
    while (itr->hasNext()) {
        itr->next();
        long s = itr->getKey();
        PyObject *value;
        if (!text) {
            value = PyLong_FromLong(s);
        } else {
            int len;
            bool resp = dict->getText(s, term, len);
            if (resp) {
                value = PyUnicode_FromStringAndSize(term, len);
            } else {
                value = PyUnicode_FromString("None");
            }
        }
        PyList_Append(obj, value);
        Py_DECREF(value);
    }
    q->releaseItr(itr);
    return obj;
}

static PyObject *db_degree(PyObject *self, PyObject *args) {
    PyObject *obj = PyList_New(0);
    KB *kb = ((trident_Db*)self)->kb;
    TreeItr *itr = kb->getItrTerms();
    TermCoordinates coord;
    while (itr->hasNext()) {
        long key = itr->next(&coord);
        long inels = 0;
        long outels = 0;
        if (coord.exists(IDX_SOP)) {
            inels = coord.getNElements(IDX_SOP);
        } else {
            if (coord.exists(IDX_SPO)) {
                inels = coord.getNElements(IDX_SPO);
            }
        }
        if (coord.exists(IDX_OPS)) {
            outels = coord.getNElements(IDX_OPS);
        } else {
            if (coord.exists(IDX_OSP)) {
                outels = coord.getNElements(IDX_OSP);
            }
        }
        PyObject *t = PyTuple_New(2);
        PyTuple_SetItem(t, 0, PyLong_FromLong(key));
        PyTuple_SetItem(t, 1, PyLong_FromLong(inels + outels));
        PyList_Append(obj, t);
        Py_DECREF(t);
    }
    delete itr;
    return obj;
}

static PyObject *db_indegree(PyObject *self, PyObject *args) {
    PyObject *obj = PyList_New(0);
    KB *kb = ((trident_Db*)self)->kb;
    TreeItr *itr = kb->getItrTerms();
    TermCoordinates coord;
    while (itr->hasNext()) {
        long key = itr->next(&coord);
        long inels = 0;
        if (coord.exists(IDX_OSP)) {
            inels = coord.getNElements(IDX_OSP);
        } else {
            if (coord.exists(IDX_OPS)) {
                inels = coord.getNElements(IDX_OPS);
            }
        }
        PyObject *t = PyTuple_New(2);
        PyTuple_SetItem(t, 0, PyLong_FromLong(key));
        PyTuple_SetItem(t, 1, PyLong_FromLong(inels));
        PyList_Append(obj, t);
        Py_DECREF(t);
    }
    delete itr;
    return obj;
}

static PyObject *db_outdegree(PyObject *self, PyObject *args) {
    PyObject *obj = PyList_New(0);
    KB *kb = ((trident_Db*)self)->kb;
    TreeItr *itr = kb->getItrTerms();
    TermCoordinates coord;
    while (itr->hasNext()) {
        long key = itr->next(&coord);
        long outels = 0;
        if (coord.exists(IDX_SOP)) {
            outels = coord.getNElements(IDX_SOP);
        } else {
            if (coord.exists(IDX_SPO)) {
                outels = coord.getNElements(IDX_SPO);
            }
        }
        PyObject *t = PyTuple_New(2);
        PyTuple_SetItem(t, 0, PyLong_FromLong(key));
        PyTuple_SetItem(t, 1, PyLong_FromLong(outels));
        PyList_Append(obj, t);
        Py_DECREF(t);
    }
    delete itr;
    return obj;
}

static PyObject *db_all(PyObject *self, PyObject *args) {
    int text = 0;
    if (!PyArg_ParseTuple(args, "|b", &text))
        return NULL;

    char term[MAX_TERM_SIZE];
    Querier *q = ((trident_Db*)self)->q;
    DictMgmt *dict =  ((trident_Db*)self)->kb->getDictMgmt();
    PyObject *obj = PyList_New(0);
    PairItr *itr = q->getPermuted(IDX_SPO, -1, -1, -1, true);
    while (itr->hasNext()) {
        itr->next();
        long s = itr->getKey();
        long p = itr->getValue1();
        long o = itr->getValue2();
        PyObject *t = PyTuple_New(3);

        if (!text) {
            PyTuple_SetItem(t, 0, PyLong_FromLong(s));
            PyTuple_SetItem(t, 1, PyLong_FromLong(p));
            PyTuple_SetItem(t, 2, PyLong_FromLong(o));
        } else {
            long ids[3];
            ids[0] = s;
            ids[1] = p;
            ids[2] = o;
            for (int i = 0; i < 3; ++i) {
                int len;
                bool resp = dict->getText(ids[i], term, len);
                if (resp) {
                    PyTuple_SetItem(t, i, PyUnicode_FromStringAndSize(term, len));
                } else {
                    PyTuple_SetItem(t, i, PyUnicode_FromString("None"));
                }
            }
        }
        PyList_Append(obj, t);
        Py_DECREF(t);
    }
    q->releaseItr(itr);
    return obj;
}

static PyObject *db_allo(PyObject *self, PyObject *args) {
    long s, p;
    if (!PyArg_ParseTuple(args, "ll", &s, &p))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);
    PairItr *itr = q->getPermuted(IDX_SPO, s, p, -1, true);
    while (itr->hasNext()) {
        itr->next();
        long o = itr->getValue2();
        PyObject *value = PyLong_FromLong(o);
        PyList_Append(obj, value);
        Py_DECREF(value);
    }

    q->releaseItr(itr);
    return obj;
}

static PyObject *db_allo_aggr(PyObject *self, PyObject *args) {
    long s;
    if (!PyArg_ParseTuple(args, "l", &s))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);
    PairItr *itr = q->getPermuted(IDX_SOP, s, -1, -1, true);
    itr->ignoreSecondColumn();
    while (itr->hasNext()) {
        itr->next();
        long o = itr->getValue1();
        PyObject *value = PyLong_FromLong(o);
        PyList_Append(obj, value);
        Py_DECREF(value);
    }

    q->releaseItr(itr);
    return obj;
}

static PyObject *db_ntriples(PyObject *self, PyObject *args) {
    KB *kb = ((trident_Db*)self)->kb;
    return PyLong_FromLong(kb->getSize());
}

static PyObject *db_nterms(PyObject *self, PyObject *args) {
    KB *kb = ((trident_Db*)self)->kb;
    return PyLong_FromLong(kb->getNTerms());
}

static PyObject *db_allpo(PyObject *self, PyObject *args) {
    long s;
    if (!PyArg_ParseTuple(args, "l", &s))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);
    PairItr *itr = q->getPermuted(IDX_SPO, s, -1, -1, true);
    while (itr->hasNext()) {
        itr->next();
        long p = itr->getValue1();
        long o = itr->getValue2();
        PyObject *t = PyTuple_New(2);
        PyTuple_SetItem(t, 0, PyLong_FromLong(p));
        PyTuple_SetItem(t, 1, PyLong_FromLong(o));
        PyList_Append(obj, t);
        Py_DECREF(t);
    }
    q->releaseItr(itr);
    return obj;
}

static PyObject *db_allps(PyObject *self, PyObject *args) {
    long o;
    if (!PyArg_ParseTuple(args, "l", &o))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);
    PairItr *itr = q->getPermuted(IDX_OPS, o, -1, -1, true);
    while (itr->hasNext()) {
        itr->next();
        long p = itr->getValue1();
        long s = itr->getValue2();
        PyObject *t = PyTuple_New(2);
        PyTuple_SetItem(t, 0, PyLong_FromLong(p));
        PyTuple_SetItem(t, 1, PyLong_FromLong(s));
        PyList_Append(obj, t);
        Py_DECREF(t);
    }
    q->releaseItr(itr);
    return obj;
}

static PyObject *db_allos(PyObject *self, PyObject *args) {
    long p;
    if (!PyArg_ParseTuple(args, "l", &p))
        return NULL;
    Querier *q = ((trident_Db*)self)->q;
    PyObject *obj = PyList_New(0);
    PairItr *itr = q->getPermuted(IDX_POS, p, -1, -1, true);
    while (itr->hasNext()) {
        itr->next();
        long o = itr->getValue1();
        long s = itr->getValue2();
        PyObject *t = PyTuple_New(2);
        PyTuple_SetItem(t, 0, PyLong_FromLong(s));
        PyTuple_SetItem(t, 1, PyLong_FromLong(o));
        PyList_Append(obj, t);
        Py_DECREF(t);
    }
    q->releaseItr(itr);
    return obj;
}

static PyObject * trident_lookup_id(PyObject *self, PyObject *args) {
    const char *term;
    if (!PyArg_ParseTuple(args, "s", &term))
        return NULL;
    KB *kb = ((trident_Db*)self)->kb;
    nTerm value;
    bool resp = kb->getDictMgmt()->getNumber(term, strlen(term), &value);
    if (resp) {
        return PyLong_FromLong(value);
    } else {
        Py_INCREF(Py_None);
        return Py_None;
    }
}

static PyObject * trident_lookup_str(PyObject *self, PyObject *args) {
    long id;
    if (!PyArg_ParseTuple(args, "l", &id))
        return NULL;
    KB *kb = ((trident_Db*)self)->kb;
    char term[MAX_TERM_SIZE];
    int len;
    bool resp = kb->getDictMgmt()->getText(id, term, len);
    if (resp) {
        return PyUnicode_FromStringAndSize(term, len);
    } else {
        Py_INCREF(Py_None);
        return Py_None;
    }
}

static PyObject * trident_search_id(PyObject *self, PyObject *args) {
    const char *term;
    if (!PyArg_ParseTuple(args, "s", &term))
        return NULL;
    KB *kb = ((trident_Db*)self)->kb;
    DictMgmt *mgmt = kb->getDictMgmt();
    TreeItr *itr = mgmt->getInvDictIterator();
    StringBuffer *sb = mgmt->getStringBuffer();
    string sTermToSearch(term);
    PyObject *obj = PyList_New(0);
    while (itr->hasNext()) {
        long value;
        long key = itr->next(value);
        int size;
        const char *text = sb->get(value, size);
        string sTerm(text, size);
        if (sTerm.find(sTermToSearch) != string::npos) {
            PyObject *t = PyTuple_New(2);
            PyTuple_SetItem(t, 0, PyLong_FromLong(key));
            PyTuple_SetItem(t, 1, PyUnicode_FromStringAndSize(text, size));
            PyList_Append(obj, t);
            Py_DECREF(t);
        }
    }
    delete itr;
    return obj;
}

static void Db_dealloc(trident_Db* self) {
    if (self->q)
        delete self->q;
    if (self->kb)
        delete self->kb;
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyMethodDef Db_methods[] = {
    {"s", db_alls, METH_VARARGS, "Get all subjects given the p and o. Returns a Python list." },
    {"s_itr", db_alls_fast, METH_VARARGS, "Get all subjects given the p and o. Returns an itr." },
    {"s_aggr", db_alls_aggr, METH_VARARGS, "Get all subjects given o" },
    {"o", db_allo, METH_VARARGS, "Get all objects given the s and p" },
    {"o_aggr", db_allo_aggr, METH_VARARGS, "Get all objects given the s" },
    {"po", db_allpo, METH_VARARGS, "Get all (predicate, objects) given s" },
    {"ps", db_allps, METH_VARARGS, "Get all (predicate, subject) given o" },
    {"os", db_allos, METH_VARARGS, "Get all (subject, object) given a p" },
    {"n_s", db_ns, METH_VARARGS, "Get the number of subjects given the p and o" },
    {"n_o", db_no, METH_VARARGS, "Get the number of objects given the s and p" },
    {"count_s", db_counts, METH_VARARGS, "Get the number of triples with the same subject" },
    {"count_o", db_counto, METH_VARARGS, "Get the number of triples with the same object" },
    {"count_p", db_countp, METH_VARARGS, "Get the number of triples with the same predicate" },
    {"exists", db_exists, METH_VARARGS, "Check if the given triple exists" },
    {"existsQuery", db_existsQuery, METH_VARARGS, "Check if the given triple exists among the results of a given pattern" },
    {"n_terms", db_nterms, METH_VARARGS, "Get the number of terms in the graph" },
    {"n_triples", db_ntriples, METH_VARARGS, "Get the number of edges in the graph" },
    {"all", db_all, METH_VARARGS, "Get the list of all triples" },
    {"all_s", db_lists, METH_VARARGS, "Get the list of all subjects" },
    {"all_p", db_listp, METH_VARARGS, "Get the list of all predicates" },
    {"all_o", db_listo, METH_VARARGS, "Get the list of all objects" },
    {"degree", db_degree, METH_VARARGS, "Get the list of all nodes with their degrees" },
    {"indegree", db_indegree, METH_VARARGS, "Get the list of all nodes with their indegrees" },
    {"outdegree", db_outdegree, METH_VARARGS, "Get the list of all nodes with their outdegrees" },
    {"lookup_id", trident_lookup_id, METH_VARARGS, "Lookup for the IDs of terms" },
    {"lookup_str", trident_lookup_str, METH_VARARGS, "Lookup for the textual term given the ID" },
    {"search_id", trident_search_id, METH_VARARGS, "Search for the IDs of terms" },
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static PyTypeObject trident_DbType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        "trident.Db",             /* tp_name */
    sizeof(trident_Db),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Db_dealloc, /* tp_dealloc */
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
    "Trident DB",           /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    Db_methods,             /* tp_methods */
    0,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Db_init,      /* tp_init */
    0,                         /* tp_alloc */
    Db_new,                 /* tp_new */
};

static struct PyModuleDef tridentmodule = {
    PyModuleDef_HEAD_INIT,
    "trident",   /* name of module */
    NULL,        /* module documentation, may be NULL */
    -1,          /* size of per-interpreter state of the module,
                    or -1 if the module keeps state in global variables. */
    NULL
};

PyMODINIT_FUNC PyInit_trident(void) {
    PyObject *m;
    m = PyModule_Create(&tridentmodule);
    if (m == NULL)
        return NULL;

    boost::shared_ptr<logging::core> core = logging::core::get();
    core->set_filter(logging::trivial::severity >= logging::trivial::info);

    trident_DbType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&trident_DbType) < 0)
        return NULL;
    if (PyType_Ready(&trident_ItrType) < 0)
        return NULL;
    if (PyType_Ready(&trident_BatcherType) < 0)
        return NULL;

    Py_INCREF(&trident_DbType);
    Py_INCREF(&trident_ItrType);
    Py_INCREF(&trident_BatcherType);
    PyModule_AddObject(m, "Db", (PyObject *)&trident_DbType);
    PyModule_AddObject(m, "Itr", (PyObject *)&trident_ItrType);
    PyModule_AddObject(m, "Batcher", (PyObject *)&trident_BatcherType);


    return m;
}

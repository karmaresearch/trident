#include <python/trident.h>
#include <Python.h>
#include <numpy/ndarrayobject.h>


#include <iostream>
#include <vector>

static PyObject *Emb_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    trident_Emb *self;
    self = (trident_Emb*)type->tp_alloc(type, 0);
    self->E = std::shared_ptr<Embeddings<double>>();
    self->R = std::shared_ptr<Embeddings<double>>();
    return (PyObject *)self;
}

static int Emb_init(trident_Emb *self, PyObject *args, PyObject *kwds) {
    const char *path = NULL;
    if (!PyArg_ParseTuple(args, "|s", &path))
        return -1;
    std::string spath = std::string(path);
    if (path != NULL) {
        self->E = Embeddings<double>::load(spath + "/E");
        self->R = Embeddings<double>::load(spath + "/R");
        std::cout << "Embeddings are loaded!" << std::endl;
    }
    import_array();
    return 0;
}

static void Emb_dealloc(trident_Emb* self) {
    self->E = std::shared_ptr<Embeddings<double>>();
    self->R = std::shared_ptr<Embeddings<double>>();
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject * emb_get_e(PyObject *self, PyObject *args) {
    int64_t id;
    if (!PyArg_ParseTuple(args, "l", &id))
        return NULL;
    npy_intp dims[1] = { ((trident_Emb*)self)->E->getDim() };
    void *data = (void*)((trident_Emb*)self)->E->get(id);
    auto obj = PyArray_SimpleNewFromData(1, dims, NPY_DOUBLE, data);
    return obj;
}

static PyObject * emb_get_r(PyObject *self, PyObject *args) {
    int64_t id;
    if (!PyArg_ParseTuple(args, "l", &id))
        return NULL;
    npy_intp dims[1] = { ((trident_Emb*)self)->R->getDim() };
    void *data = (void*)((trident_Emb*)self)->R->get(id);
    auto obj = PyArray_SimpleNewFromData(1, dims, NPY_DOUBLE, data);
    return obj;
}

static PyObject * emb_get_info_e(PyObject *self, PyObject *args) {
    int64_t id;
    if (!PyArg_ParseTuple(args, "l", &id))
        return NULL;
    uint64_t conflicts = ((trident_Emb*)self)->E->getNConflicts(id);
    uint64_t updates = ((trident_Emb*)self)->E->getNUpdates(id);

    PyObject *obj = PyList_New(0);
    PyObject *value = PyLong_FromLong(conflicts);
    PyList_Append(obj, value);
    Py_DECREF(value);
    value = PyLong_FromLong(updates);
    PyList_Append(obj, value);
    Py_DECREF(value);
    return obj;
}

static PyObject * emb_get_info_r(PyObject *self, PyObject *args) {
    int64_t id;
    if (!PyArg_ParseTuple(args, "l", &id))
        return NULL;
    uint64_t conflicts = ((trident_Emb*)self)->R->getNConflicts(id);
    uint64_t updates = ((trident_Emb*)self)->R->getNUpdates(id);

    PyObject *obj = PyList_New(0);
    PyObject *value = PyLong_FromLong(conflicts);
    PyList_Append(obj, value);
    Py_DECREF(value);
    value = PyLong_FromLong(updates);
    PyList_Append(obj, value);
    Py_DECREF(value);
    return obj;
}

static PyMethodDef Emb_methods[] = {
    {"get_e", emb_get_e, METH_VARARGS, "Get the embedding of the entity" },
    {"get_info_e", emb_get_info_e, METH_VARARGS, "Get info of the entity" },
    {"get_r", emb_get_r, METH_VARARGS, "Get the embedding of the relation" },
    {"get_info_e", emb_get_info_r, METH_VARARGS, "Get info of the relation" },
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyTypeObject trident_EmbType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        "trident.Emb",             /* tp_name */
    sizeof(trident_Emb),       /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Emb_dealloc,   /* tp_dealloc */
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
    "Trident Embeddings",        /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    Emb_methods,                /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Emb_init,        /* tp_init */
    0,                         /* tp_alloc */
    Emb_new,                   /* tp_new */
};

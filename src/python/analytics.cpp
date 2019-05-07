#include <python/trident.h>

#include <trident/kb/kb.h>

#include <snap/directed.h>
#include <snap-core/Snap.h>

#include <Python.h>
#include <numpy/ndarrayobject.h>
#include <vector>

static PyObject *ana_ppr(PyObject *self, PyObject *args) {
    double C = 0.85;
    double eps = 0;
    int maxiter = 100;
    trident_Db *pkb = NULL;
    KB *kb = NULL;
    PyArrayObject *npNodesWeights = NULL;
    PyArrayObject *npOutDegrees = NULL;

    if (!PyArg_ParseTuple(args, "O!O!O!|ddi", &trident_DbType, &pkb,
                &PyArray_Type,
                &npNodesWeights,
                &PyArray_Type,
                &npOutDegrees,
                &C, &eps, &maxiter
                )) {
        std::cerr << "Errors in parsing the arguments" << std::endl;
        return NULL;
    }
    kb = pkb->kb;

    //Check the type of elements in the array
    int typ=PyArray_TYPE(npNodesWeights);
    if (!PyTypeNum_ISFLOAT(typ)) {
        PyErr_SetString(PyExc_BaseException, "The array with the weights should contain"
                " 4bytes float numbers.");
        Py_INCREF(Py_None);
        return Py_None;
    }
    if (typ != NPY_DOUBLE) {
        PyErr_SetString(PyExc_BaseException, "The array with the weights should contain"
                " 64bytes float numbers.");
        Py_INCREF(Py_None);
        return Py_None;
    }

    //Check that the array is large enough
    auto nels = PyArray_SIZE(npNodesWeights);
    if (nels < kb->getNTerms()) {
        std::string err = "The array with the weights contains only " +
            to_string(nels) +
            ". Should contain " + to_string(kb->getNTerms());
        PyErr_SetString(PyExc_BaseException, err.c_str());
        Py_INCREF(Py_None);
        return Py_None;
    }

    auto nels2 = PyArray_SIZE(npOutDegrees);
    if (nels2 < nels) {
        std::string err = "The array with the outdegree contains only " +
            to_string(nels) +
            ". Should contain " + to_string(kb->getNTerms());
        PyErr_SetString(PyExc_BaseException, err.c_str());
        Py_INCREF(Py_None);
        return Py_None;
    }

    double *importanceNodes = (double*)PyArray_DATA(npNodesWeights);
    uint64_t *weights = (uint64_t*)PyArray_DATA(npOutDegrees);

    PTrident_TNGraph graph = new Trident_TNGraph(kb);
    TSnap::GetPageRank_stl_raw<PTrident_TNGraph>(graph,
            importanceNodes,
            weights,
            false, //if true then init values1
            C,
            eps,
            maxiter);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyMethodDef AnalyticsFunctions[] = {
    {"ppr", ana_ppr, METH_VARARGS, "Launch Personalized PageRank" },
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef TridentAnaModule = {
    PyModuleDef_HEAD_INIT,
    "analytics",   /* name of module */
    NULL,        /* module documentation, may be NULL */
    -1,          /* size of per-interpreter state of the module,
                    or -1 if the module keeps state in global variables. */
    AnalyticsFunctions,
    NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC PyInit_analytics(void) {
    if(PyArray_API == NULL) {
        import_array();
    }
    return PyModule_Create(&TridentAnaModule);
}

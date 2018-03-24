#include <Python.h>

#include <trident/kb/kb.h>
#include <python/trident.h>

#include <snap/directed.h>
#include <snap-core/Snap.h>
#include <vector>

static PyObject *ana_ppr(PyObject *self, PyObject *args) {
    double C = 0.85;
    double eps = 0;
    int maxiter = 100;
    trident_Db *pkb;
    KB *kb = NULL;
    std::vector<float> importanceNodes;

    if (!PyArg_ParseTuple(args, "O!|ddi", &trident_DbType, &pkb, &C, &eps, &maxiter))
        return NULL;

    kb = pkb->kb;
    PTrident_TNGraph graph = new Trident_TNGraph(kb);
    TSnap::GetPageRank_stl<PTrident_TNGraph>(graph,
            importanceNodes,
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
    return PyModule_Create(&TridentAnaModule);
}

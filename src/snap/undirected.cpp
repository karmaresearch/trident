#include <snap/directed.h>
#include <snap/undirected.h>

Trident_UTNGraph::Trident_UTNGraph(KB *kb) {
    this->kb = kb;
    this->q = kb->query();
    SnapReaders::loadAllFiles(kb);
    string path = kb->getPath() + string("/tree/flat");
    mf = std::unique_ptr<MemoryMappedFile>(new MemoryMappedFile(path));
    rawnodes = mf->getData();
    nnodes = mf->getLength() / 18;
}

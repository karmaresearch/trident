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
    nnodes = (uint64_t)mapped_rgn.get_size() / 18;
}

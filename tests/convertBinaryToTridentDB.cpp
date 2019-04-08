#include <trident/kb/kb.h>
#include <trident/ml/batch.h>

#include <fstream>


int main(int argc, const char** argv) {
    std::string dbdir = argv[1];
    std::string outdir = argv[2];
    KBConfig config;
    KB kb(dbdir.c_str(), true, false, true, config);

    //Load the binary file
    std::string batchfile = dbdir + "/_batch";

    //Read all the triples, create a file
    std::vector<uint64_t> traintriples;
    BatchCreator::loadTriples(batchfile, traintriples);
    std::cout << "contans " << (traintriples.size() / 3) << std::endl;

    //Write the file with the triples
    std::string outfile_path = outdir + "/triples";
    std::ofstream outfile = std::ofstream(outfile_path, std::ofstream::out);
    for(uint64_t i = 0; i < traintriples.size(); i+=3) {
        std::string line = std::to_string(traintriples[i]) + " " + std::to_string(traintriples[i+1]) + " " + std::to_string(traintriples[i+2]);
        outfile << line << std::endl;
    }
    outfile.close();

    //Write the file with the dictionary
    std::string dict_path = outdir + "/dict";
    std::ofstream dictfile = std::ofstream(dict_path, std::ofstream::out);
    auto dict = kb.getDictMgmt();
    char buffer[MAX_TERM_SIZE];
    int sizebuffer = 0;
    uint64_t largestID = kb.getNTerms();
    std::cout << "LargestID " << largestID << std::endl;
    for(uint64_t i = 0; i <= largestID; ++i) {
        //Get the text
        bool resp = dict->getText(i, buffer, sizebuffer);
        if (!resp) {
            std::cout << "ID " << i << " does not exist" << std::endl;
        } else {
            std::string line = std::to_string(i) + " " + std::to_string(sizebuffer) + " " + std::string(buffer, sizebuffer);
            dictfile << line << std::endl;
        }
    }
    dictfile.close();

    //Write the file with the relation dictionary
    std::string dict_rel_path = outdir + "/dict_rel";
    std::ofstream dictfile_rel = std::ofstream(dict_rel_path, std::ofstream::out);
    uint64_t nrels = kb.getNRels();
    for(uint64_t i = 0; i < nrels; ++i) {
        bool resp = dict->getTextRel(i, buffer, sizebuffer);
        if (!resp) {
            std::cout << "Rel ID " << i << " does not exist" << std::endl;
        } else {
            std::string line = std::to_string(i) + " " + std::to_string(sizebuffer) + " " + std::string(buffer, sizebuffer);
            dictfile_rel << line << std::endl;
        }
    }
    dictfile_rel.close();
}

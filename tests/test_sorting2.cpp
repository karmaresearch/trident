#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>

#include <kognac/diskreader.h>
#include <kognac/disklz4writer.h>
#include <kognac/disklz4reader.h>
#include <kognac/multidisklz4writer.h>
#include <kognac/multidisklz4reader.h>
#include <kognac/multimergedisklz4reader.h>
#include <kognac/compressor.h>
#include <kognac/compressor.h>
#include <kognac/filereader.h>
#include <kognac/schemaextractor.h>
#include <kognac/triplewriters.h>
#include <kognac/utils.h>
#include <kognac/hashfunctions.h>
#include <kognac/factory.h>
#include <kognac/lruset.h>
#include <kognac/flajolet.h>
#include <kognac/stringscol.h>
#include <kognac/MisraGries.h>
#include <kognac/sorter.h>
#include <kognac/filemerger.h>
#include <kognac/filemerger2.h>
#include <kognac/logs.h>


using namespace std;

void sortPart(ParamsSortPartition params) {
    string prefixInputFiles = params.prefixInputFiles;
    MultiDiskLZ4Reader *reader = params.reader;
    MultiMergeDiskLZ4Reader *mergerReader = params.mergerReader;
    int idWriter = params.idWriter;
    string prefixIntFiles = params.prefixIntFiles;
    int part = params.part;
    int64_t maxMem = params.maxMem;

    std::vector<string> filesToSort;

    string parentDir = Utils::parentDir(prefixInputFiles);
    std::vector<string> children = Utils::getFiles(parentDir);
    for (auto pfile : children) {
        if (Utils::isFile(pfile)) {
            if (Utils::contains(pfile, "range")) {
                if (Utils::hasExtension(pfile)) {
                    string ext = Utils.extension(pfile);
                    if (ext == string(".") + to_string(part)) {
                        if (UtilsSfile_size() > 0) {
                            filesToSort.push_back(pfile);
                        } else {
                            Utils::remove(pfile);
                        }
                    }
                }
            }
        }
    }
    reader->addInput(idWriter, filesToSort);

    string outputFile = prefixIntFiles + "tmp";

    //Keep all the prefixes stored in a map to increase the size of URIs we can
    //keep in main memory
    StringCollection colprefixes(4 * 1024 * 1024);
    ByteArraySet prefixset;
    prefixset.set_empty_key(EMPTY_KEY);

    StringCollection col(128 * 1024 * 1024);
    std::vector<SimplifiedAnnotatedTerm> tuples;
    std::vector<string> sortedFiles;
    int64_t bytesAllocated = 0;
    int idx = 0;
    std::unique_ptr<char[]> tmpprefix = std::unique_ptr<char[]>(new char[MAX_TERM_SIZE]);

    //Load all the files until I fill main memory.
    while (!reader->isEOF(idWriter)) {
        SimplifiedAnnotatedTerm t;
        t.readFrom(idWriter, reader);
        assert(t.prefix == NULL);
        if ((bytesAllocated +
                    (sizeof(SimplifiedAnnotatedTerm) * tuples.size()))
                >= maxMem) {
            LOG(DEBUGL) << "Dumping file " << idx << " with " << tuples.size() << " tuples ...";

            string ofile = outputFile + string(".") + to_string(idx);
            idx++;
            Compressor::sortAndDumpToFile(tuples, ofile, false);
            sortedFiles.push_back(ofile);
            tuples.clear();
            col.clear();
            bytesAllocated = 0;

        }

        //Check if I can compress the prefix of the string
        int sizeprefix = 0;
        const char *prefix = t.getPrefix(sizeprefix);
        if (sizeprefix > 4) {
            //Check if the prefix exists in the map
            Utils::encode_short(tmpprefix.get(), sizeprefix);
            memcpy(tmpprefix.get() + 2, prefix, sizeprefix);
            auto itr = prefixset.find((const char*)tmpprefix.get());
            if (itr == prefixset.end()) {
                t.term = col.addNew((char*) t.term +  sizeprefix,
                        t.size - sizeprefix);
                t.size = t.size - sizeprefix;
                const char *prefixtoadd = colprefixes.addNew(tmpprefix.get(),
                        sizeprefix + 2);
                t.prefix = prefixtoadd;
                prefixset.insert(prefixtoadd);
                assert(Utils::decode_short(t.prefix) > 0);
            } else {
                t.prefix = *itr;
                t.term = col.addNew((char*) t.term +  sizeprefix,
                        t.size - sizeprefix);
                t.size = t.size - sizeprefix;
                assert(Utils::decode_short(t.prefix) > 0);
            }

        } else {
            t.prefix = NULL;
            t.term = col.addNew((char *) t.term, t.size);
        }

        tuples.push_back(t);
        bytesAllocated += t.size;
        }

           if (tuples.size() > 0) {
                string ofile = outputFile + string(".") + to_string(idx);
                Compressor::sortAndDumpToFile(tuples, ofile, false);
                sortedFiles.push_back(ofile);
            }

        LOG(DEBUGL) << "Number of prefixes " << prefixset.size();
}

void mergePart(ParamsSortPartition params) {

	int idx = 100;
    string prefixInputFiles = params.prefixInputFiles;
    MultiDiskLZ4Reader *reader = params.reader;
    MultiMergeDiskLZ4Reader *mergerReader = params.mergerReader;
    int idWriter = params.idWriter;
    string prefixIntFiles = params.prefixIntFiles;
    int part = params.part;
    int64_t maxMem = params.maxMem;


    	std::vector<string> sortedFiles;
	int partId = params.part;
	string outputFile = params.prefixIntFiles + "tmp";
	string parentDir = Utils::parentDir(prefixInputFiles);
	std::vector<string> children = Utils::getFiles(parentDir);
	for (auto pfile : children) {
	    if (Utils::isFile(pfile)) {
            if (Utils::contains(pfile, outputFile)) {
                            sortedFiles.push_back(pfile);
            }
        }
    }
	

        LOG(DEBUGL) << "Merge " << sortedFiles.size()
                << " files in order to sort the partition";

            while (sortedFiles.size() >= 4) {
                //Add files to the batch
                std::vector<string> batchFiles;
                batchFiles.push_back(sortedFiles[0]);
                std::vector<string> cont1;
                cont1.push_back(sortedFiles[0]);
                mergerReader->addInput(idWriter * 3, cont1);

                std::vector<string> cont2;
                cont2.push_back(sortedFiles[1]);
                batchFiles.push_back(sortedFiles[1]);
                mergerReader->addInput(idWriter * 3 + 1, cont2);

                std::vector<string> cont3;
                cont3.push_back(sortedFiles[2]);
                batchFiles.push_back(sortedFiles[2]);
                mergerReader->addInput(idWriter * 3 + 2, cont3);

                //Create output file
                string ofile = outputFile + string(".") + to_string(++idx);
                LZ4Writer writer(ofile);

                //Merge batch of files
                FileMerger2<SimplifiedAnnotatedTerm> merger(mergerReader,
                        idWriter * 3, 3);
                while (!merger.isEmpty()) {
                    SimplifiedAnnotatedTerm t = merger.get();
                    t.writeTo(&writer);
                }
                mergerReader->unsetPartition(idWriter * 3);
                mergerReader->unsetPartition(idWriter * 3 + 1);
                mergerReader->unsetPartition(idWriter * 3 + 2);

                //Remove them
                sortedFiles.push_back(ofile);
                for (auto f : batchFiles) {
                    Utils::remove(f);
                    sortedFiles.erase(sortedFiles.begin());
                }
            }
            LOG(DEBUGL) << "Final merge";

            const char *prevPrefix = NULL;
            char *previousTerm = new char[MAX_TERM_SIZE];
            int previousTermSize = 0;
            int64_t counterTerms = -1;
            int64_t counterPairs = 0;
            //Sort the files
            for (int i = 0; i < sortedFiles.size(); ++i) {
                std::vector<string> cont1;
                cont1.push_back(sortedFiles[i]);
                mergerReader->addInput(idWriter * 3 + i, cont1);
            }

            FileMerger2<SimplifiedAnnotatedTerm> merger(mergerReader,
                    idWriter * 3, sortedFiles.size());
            while (!merger.isEmpty()) {
                SimplifiedAnnotatedTerm t = merger.get();
                if (!t.equals(previousTerm, previousTermSize, prevPrefix)) {
                    counterTerms++;

                    memcpy(previousTerm, t.term, t.size);
                    prevPrefix = t.prefix;
                    previousTermSize = t.size;

                    //dictWriter->writeLong(idDictWriter, counterTerms);
                    //if (t.prefix == NULL) {
                    //    dictWriter->writeString(idDictWriter, t.term, t.size);
                    //} else {
                    //    int lenprefix = Utils::decode_short(t.prefix);
                    //    int64_t len = lenprefix + t.size;
                    //    dictWriter->writeVLong(idDictWriter, len);
                    //    dictWriter->writeRawArray(idDictWriter, t.prefix + 2,
                    //            lenprefix);
                    //    dictWriter->writeRawArray(idDictWriter, t.term, t.size);
                    //}
                }

                //Write the output
                counterPairs++;
                //assert(t.tripleIdAndPosition != -1);
                //writer->writeLong(idWriter, counterTerms);
                //writer->writeLong(idWriter, t.tripleIdAndPosition);
            }

}

int main(int argc, const char** argv) {
	string prefixInputFile = argv[1];
	string dictfile = argv[2];
	string outputfile = argv[3];

	int maxReadingThreads = 8;
	int partitions = 72;
	int64_t maxMem = (int64_t)10521374418;

        std::vector<uint64_t> counters(partitions);
        std::vector<std::thread> threads(partitions);
        std::vector<string> outputfiles;
	DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
        MultiDiskLZ4Writer **dictwriters = new MultiDiskLZ4Writer*[maxReadingThreads];
        MultiDiskLZ4Reader **mreaders = new MultiDiskLZ4Reader*[maxReadingThreads];
        MultiMergeDiskLZ4Reader **mergereaders = new MultiMergeDiskLZ4Reader*[maxReadingThreads];
        for (int i = 0; i < maxReadingThreads; ++i) {
            string out = prefixInputFile + "-sortedpart-" + to_string(i);
            outputfiles.push_back(out);
            writers[i] = new DiskLZ4Writer(out, partitions / maxReadingThreads, 3);
            mreaders[i] = new MultiDiskLZ4Reader(partitions / maxReadingThreads, 3, 4);
            mreaders[i]->start();
            mergereaders[i] = new MultiMergeDiskLZ4Reader(
                    partitions / maxReadingThreads * 3, 2, 4);
            mergereaders[i]->start();

            std::vector<string> dictfiles;
            int filesPerPart = partitions / maxReadingThreads;
            for (int j = 0; j < filesPerPart; ++j) {
                string dictpartfile = dictfile + string(".") +
                    to_string(j * maxReadingThreads + i);
                dictfiles.push_back(dictpartfile);
            }
            dictwriters[i] = new MultiDiskLZ4Writer(dictfiles, 3, 4);
        }

        for (int i = 0; i < partitions; ++i) {
            ParamsSortPartition params;
            params.prefixInputFiles = prefixInputFile;
            params.reader = mreaders[i % maxReadingThreads];
            params.mergerReader = mergereaders[i % maxReadingThreads];
            params.dictWriter = dictwriters[i % maxReadingThreads];
            params.idDictWriter = i / maxReadingThreads;
            params.writer = writers[i % maxReadingThreads];
            params.idWriter = i / maxReadingThreads;
            params.prefixIntFiles = outputfiles[i % maxReadingThreads] + to_string(i);
            params.part = i;
            params.counter = &counters[i];
            params.maxMem = maxMem;

            threads[i] = std::thread(std::bind(mergePart, params));
        }
        for (int i = 0; i < partitions; ++i) {
            threads[i].join();
        }

	return 0;
}

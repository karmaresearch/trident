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

#include <trident/loader.h>

using namespace std;

void generateNewPerm_seq(MultiDiskLZ4Reader *reader,
        MultiDiskLZ4Writer *writer,
        int idx,
        int pos0,
        int pos1,
        int pos2) {
    	int64_t triple[3];
	int64_t count = 0;
    	while (!reader->isEOF(idx)) {
        	Triple t;
        	t.readFrom(idx, reader);
        	triple[0] = t.s;
        	triple[1] = t.p;
        	triple[2] = t.o;
		count++;
		if (count >= 126626764)
			LOG(DEBUGL) << "Read " << count << " " << idx;
    	}
LOG(DEBUGL) << "End thread";
}

void generateNewPerm(string outputdir,
        string inputdir,
        char pos0,
        char pos1,
        char pos2,
        int parallelProcesses,
        int maxReadingThreads) {
    //Read all files in the the directory.
    LOG(DEBUGL) << "Start process generating new permutation";
    if (Utils::isDirectory(inputDir)) {
        auto files = Utils::getFiles(inputDir);
        std::vector<std::vector<string>> chunks(parallelProcesses);
        int idx = 0;
        for (auto f : files) {
            chunks[idx].push_back(f);
            idx = (idx + 1) % parallelProcesses;
        }

        //Setup readers
        MultiDiskLZ4Reader **readers = new MultiDiskLZ4Reader*[maxReadingThreads];
        int partsPerFiles = parallelProcesses / maxReadingThreads;
        idx = 0;
        for(int i = 0; i < 1; ++i) {
            readers[i] = new MultiDiskLZ4Reader(partsPerFiles, 3, 4);
            readers[i]->start();
            for(int j = 0; j < partsPerFiles; ++j)
                readers[i]->addInput(j, chunks[idx++]);
        }

        //Setup writers
        MultiDiskLZ4Writer **writers = new MultiDiskLZ4Writer*[maxReadingThreads];
        idx = 0;
        for (int i = 0; i < maxReadingThreads; ++i) {
            std::vector<string> outputchunk;
            for(int j = 0; j < partsPerFiles; ++j) {
                outputchunk.push_back(outputdir + "/input-" + to_string(idx++));
            }
            writers[i] = new MultiDiskLZ4Writer(outputchunk, 3, 4);
        }
        //Start threads
        std::thread *threads = new std::thread[parallelProcesses];
        for(int i = 0; i < 8; i+=2) {
            int idx1 = i % maxReadingThreads;
            int idx2 = i / maxReadingThreads;
            MultiDiskLZ4Reader *reader = readers[idx1];
            MultiDiskLZ4Writer *writer = writers[idx1];
	if (i == 6)
            threads[i] = std::thread(generateNewPerm_seq,
                    reader,
                    writer,
                    idx2,
                    pos0,
                    pos1,
                    pos2);
        }
        for(int i = 0; i < parallelProcesses; ++i) {
            threads[i].join();
        }

	LOG(DEBUGL) << "Going to delete the readers";
        for(int i = 0; i < maxReadingThreads; ++i) {
            delete readers[i];
            delete writers[i];
        }
        delete[] readers;
        delete[] writers;
        delete[] threads;
    }
    LOG(DEBUGL) << "Finished";
}

int main(int argc, const char** argv) {
	string inputfile = argv[1];
	string outputfile = argv[2];
	int maxReadingThreads = 2;
	int partitions = 8;
	generateNewPerm(outputfile, inputfile, 0, 1, 2, partitions, maxReadingThreads);
	return 0;
}

#include "../src/kb/kbconfig.h"
#include "../src/kb/kb.h"
#include "../src/kb/memoryopt.h"
#include "../src/kb/inserter.h"

#include "../src/main/loader.h"

#include "../src/sorting/sorter.h"
#include "../src/sorting/filemerger.h"

#include "../src/compression/compressor.h"

#include "../src/utils/lz4io.h"
#include "../src/utils/utils.h"

#include "../src/utils/stringscol.h"

#include <boost/chrono.hpp>
#include <boost/filesystem.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

using namespace std;
namespace timens = boost::chrono;
namespace fs = boost::filesystem;

void newCompressTriples(ParamsNewCompressProcedure params) {

//Read the file in input
	string in = params.inNames[params.part];
	fs::path pFile(in);
	fs::path pNewFile = pFile;
	pNewFile.replace_extension(to_string(params.itrN));
	string newFile = pNewFile.string();
	int64_t compressedTriples = 0;
	int64_t compressedTerms = 0;
	int64_t uncompressedTerms = 0;
	LZ4Reader *uncommonTermsReader = NULL;

	int64_t nextTripleId = -1;
	int nextPos = -1;
	int64_t nextTerm = -1;
	if (params.uncommonTermsFile != NULL) {
		BOOST_LOG_TRIVIAL(debug)<< "I'm going to use " << *(params.uncommonTermsFile) << " to process the unfrequent terms";
		uncommonTermsReader = new LZ4Reader(*(params.uncommonTermsFile));
		if (!uncommonTermsReader->isEof()) {
			int64_t tripleId = uncommonTermsReader->parseLong();
			nextTripleId = tripleId >> 2;
			nextPos = tripleId & 0x3;
			nextTerm = uncommonTermsReader->parseLong();
			BOOST_LOG_TRIVIAL(debug) << "First triple is " << nextTripleId << " " << nextPos;
		} else {
			BOOST_LOG_TRIVIAL(warning)<< "The file " << *(params.uncommonTermsFile) << " is empty";
		}
	} else {
		BOOST_LOG_TRIVIAL(debug) << "No uncommon file is provided";
	}

	int64_t currentTripleId = params.part;
	int increment = params.parallelProcesses;
	BOOST_LOG_TRIVIAL(debug)<<"My partition " << currentTripleId << " increment " << increment;

	if (fs::exists(pFile) && fs::file_size(pFile) > 0) {
		LZ4Reader r(pFile.string());
		LZ4Writer w(newFile);

		int64_t triple[3];
		char *tTriple = new char[MAX_TERM_SIZE * 3];
		bool valid[3];

		SimpleTripleWriter spoWriter(params.SPOoutputDir,
				params.prefixOutputFile + to_string(params.part));
		SimpleTripleWriter opsWriter(params.OPSoutputDir,
				params.prefixOutputFile + to_string(params.part));

		int64_t processedTriples = 0;

		while (!r.isEof()) {
			for (int i = 0; i < 3; ++i) {
				valid[i] = false;
				int flag = r.parseByte();
				if (flag == 1) {
					//convert number
					triple[i] = r.parseLong();
					valid[i] = true;
				} else {
					//Match the text against the hashmap
					int size;
					const char *tTerm = r.parseString(size);

					if (currentTripleId == nextTripleId && nextPos == i) {
						triple[i] = nextTerm;
						valid[i] = true;
						if (!uncommonTermsReader->isEof()) {
							int64_t tripleId = uncommonTermsReader->parseLong();
							if (tripleId < 0) {
								cout << "TripleId == " << tripleId << endl;
								exit(1);
							}
							nextTripleId = tripleId >> 2;

							if (nextTripleId < 0) {
								cout << "NextTripleId == " << nextTripleId << " " << tripleId << endl;
								exit(1);
							}

							nextPos = tripleId & 0x3;
							nextTerm = uncommonTermsReader->parseLong();
						} else {
							BOOST_LOG_TRIVIAL(debug)<< "nexTripleId=" << nextTripleId << " pos " << nextPos << " was the last element of uncommon file";
						}
						compressedTerms++;
					} else {
						bool ok = false;
						//Check the hashmap
						if (params.commonMap != NULL) {
							ByteArrayToNumberMap::iterator itr =
									params.commonMap->find(tTerm);
							if (itr != params.commonMap->end()) {
								triple[i] = itr->second;
								valid[i] = true;
								ok = true;
								compressedTerms++;
							}
						}

						if (!ok) {
							CompressedByteArrayToNumberMap::iterator itr2 =
									params.map->find(tTerm);
							if (itr2 != params.map->end()) {
								triple[i] = itr2->second;
								valid[i] = true;
								compressedTerms++;
							} else {
								memcpy(tTriple + MAX_TERM_SIZE * i, tTerm,
										size);
								uncompressedTerms++;
								cout << "Could not find the term "
										<< string(tTerm + 2, size - 2) << " "
										<< processedTriples << "nextTriple="
										<< nextTripleId << " pos=" << nextPos
										<< " currentTripleId="
										<< currentTripleId << endl;
								exit(1);
							}
						}
					}
				}
			}

			if (valid[0] && valid[1] && valid[2]) {
				spoWriter.write(triple[0], triple[1], triple[2]);
				opsWriter.write(triple[2], triple[1], triple[0]);
				compressedTriples++;
			} else {
				//Write it into the file
				for (int i = 0; i < 3; ++i) {
					if (valid[i]) {
						w.writeByte(1);
						w.writeLong((int64_t) triple[i]);
					} else {
						w.writeByte(0);
						char *t = tTriple + MAX_TERM_SIZE * i;
						w.writeString(t, Utils::decode_short(t) + 2);
					}
				}
			}

			currentTripleId += increment;

			processedTriples++;
			if (processedTriples % 1000000 == 0) {
				cout << "Processed triples " << processedTriples << " "
						<< uncompressedTerms << endl;
			}
		}

		if (uncommonTermsReader != NULL) {
			if (!(uncommonTermsReader->isEof())) {
				BOOST_LOG_TRIVIAL(error)<< "There are still elements to read in the uncommon file";
			}
			delete uncommonTermsReader;
		}
		delete[] tTriple;
	} else {
		BOOST_LOG_TRIVIAL(warning)<< "The file " << in << " does not exist or is empty";
	}

	BOOST_LOG_TRIVIAL(debug)<< "Compressed triples " << compressedTriples << " compressed terms " << compressedTerms << " uncompressed terms " << uncompressedTerms;

	//Delete the input file and replace it with a new one
//	fs::remove(pFile);
	params.inNames[params.part] = newFile;
}

void loadMap(CompressedByteArrayToNumberMap &uncommonMap, string *files,
		int len, StringCollection &buffer) {
	for (int i = 0; i < len; ++i) {
		LZ4Reader *r = new LZ4Reader(files[i]);
		int j = 0;
		while (!(r->isEof()) && j < 500) {
			nTerm term = r->parseLong();
			int sizeTerm;
			const char *tTerm = r->parseString(sizeTerm);
			const char *tTerm2 = buffer.addNew(tTerm, sizeTerm);
			uncommonMap.insert(std::make_pair(tTerm2, term));
			j++;
		}
		delete r;

		r = new LZ4Reader(files[i] + string("-np1"));
		while (!(r->isEof())) {
			nTerm term = r->parseLong();
			int sizeTerm;
			const char *tTerm = r->parseString(sizeTerm);
			const char *tTerm2 = buffer.addNew(tTerm, sizeTerm);
			uncommonMap.insert(std::make_pair(tTerm2, term));
		}
		delete r;
	}
}

int main(int argc, const char** argv) {
	ParamsNewCompressProcedure params;

	params.part = atoi(argv[1]);
	params.inNames = new string(argv[2]);
	params.uncommonTermsFile = new string(argv[3]);
	params.SPOoutputDir = string(argv[4]);
	params.OPSoutputDir = string(argv[5]);

	params.prefixOutputFile = "p";
	params.parallelProcesses = 16;
	params.itrN = 0;
	params.commonMap = NULL;

	CompressedByteArrayToNumberMap uncommonMap;
	params.map = &uncommonMap;

	//Load the uncommmonMap
	StringCollection col(64000000);
	int len = 4;
	string *files = new string[len];
	files[0] = string(argv[6]);
	files[1] = string(argv[7]);
	files[2] = string(argv[8]);
	files[3] = string(argv[9]);
	loadMap(uncommonMap, files, len, col);

	newCompressTriples(params);
	BOOST_LOG_TRIVIAL(debug)<< "finished";
}

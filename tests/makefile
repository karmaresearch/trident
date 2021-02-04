BUILD=build
KOGNAC=../../kognac
KOGNAC_BUILDDIR=$(KOGNAC)/$(BUILD)
TRIDENT_BUILDDIR=../$(BUILD)
CINCLUDES= -I../include -I$(KOGNAC)/include -I../snap -I../snap/snap-core -I../snap/glib-core -I$(KOGNAC_BUILDDIR)/external/lz4/lib -I$(KOGNAC_BUILDDIR)/external/sparsehash/src
CLIBS=-L$(KOGNAC_BUILDDIR) -L$(TRIDENT_BUILDDIR) -L../snap/snap-core -L$(KOGNAC_BUILDDIR)/external/lz4/lib -L$(KOGNAC_BUILDDIR)/external/sparsehash/src -ltrident-core -lkognac-core
CPLUS=g++
GOOGLE=-I /Users/jacopo/prj/kognac/build/external/sparsehash/src

test_hash:
	$(CPLUS) $(GOOGLE) -I/Users/jacopo/prj/vlog/include -fPIC -std=c++11 -DPRUNING_QSQR=1 -o ./testHash -O3 test_hashnumbers.cpp

test_json:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testJSON -std=c++0x -O0 test_json.cpp -lpthread

test_insert8:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testInsert8 -std=c++0x -O3 test_insert8.cpp -lpthread

test_insert6:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testInsert6 -std=c++0x -O3 test_insert6.cpp -lpthread

test_sorting4:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testSorting4 -std=c++0x -g -O0 test_sorting4.cpp -lpthread

test_ppr:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testPPR -std=c++0x -O0 -g test_ppr.cpp -ltrident-ana

test_httpclient:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testHttpClient -std=c++0x -O0 -g test_httpclient.cpp

test_httpserver:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testHttp -std=c++0x -O0 -g test_httpserver.cpp -ltrident-web

test_multi:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testMulti -std=c++0x  -O0 -g test_multi.cpp -lpthread -llz4 -lsnap

test_transe4:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testTranse4 -std=c++0x -g -O0 test_transe4.cpp -lpthread  -llz4 -lsnap

test_transe3:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testTranse3 -std=c++0x -g -O0 test_transe3.cpp -lpthread  -llz4 -lsnap

test_transe2:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testTranse2 -std=c++0x -g -O0 test_transe2.cpp -lpthread  -llz4 -lsnap

test_transe:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testTranse -std=c++0x -g -O0 test_transe.cpp -lpthread  -llz4 -lsnap

test_snap:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testSnap -std=c++0x  -O3 test_snap.cpp -llz4 -lsnap -lgomp -lpthread

test_insert5:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testInsert5 -std=c++0x  -O0 test_insert5.cpp -lpthread -llz4

test_treeitr:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o treeItr -std=c++0x  -O0 test_treeitr.cpp -lpthread -llz4

test_insertlarge:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o testInsertlarge  -O3 test_insertlarge.cpp -lpthread -llz4

test_graph:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o testGraph -O3 test_graph.cpp -lsnap

test_generate:
	g++ $(CINCLUDES) $(CLIBS) -o testGenerate  -O0 -g test_generate.cpp -llz4 -lpthread

test_sorting2:
	g++ $(CINCLUDES) $(CLIBS) -o testSorting2  -O0 -g test_sorting2.cpp -llz4 -lpthread

test_pfor:
	g++ $(CINCLUDES) $(CLIBS) -o testpfor -O3 -std=c++0x -g test_parallel_for.cpp

test_sorting3:
	g++ $(CINCLUDES) $(CLIBS) -o testSorting3 -O3 -std=c++0x -g test_sorting3.cpp
test_io:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -llz4 -lpthread -o test_io  -O3 test_io.cpp -std=c++0x

test_reasoner:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -llz4 -lpthread -o test -O0 test_reasoner.cpp -std=c++0x

testinsertperm:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -o testIndexPerm -llz4 -lpthread test_insert_perm.cpp -std=c++0x

testquerytree:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o testTree -O3 -llz4 -lpthread test_querytree.cpp -std=c++0x

testdictionary:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o testDictionary -lpthread test_dictionary_tree.cpp -std=c++0x

testcache:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -o testCache -std=c++0x test_cache.cpp

testnumbers:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -o testNumber -lpthread -llz4 test_number.cpp -std=c++0x

testindex:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -o testIndex -lpthread test_index.cpp -std=c++0x

testmergesort:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -o testMerge -lpthread test_mergesort.cpp -std=c++0x

testinsertdictionary:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -g -o testInsertDictionary -llz4 -lpthread test_insertdictionary.cpp -std=c++0x

testinserttree:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -g -o testInsertTree -llz4 -lz -lpthread test_insert_tree.cpp -std=c++0x

testinsertindex:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -g -o testInsertIndex -llz4 -lpthread test_insertindex.cpp -std=c++0x

testlz4:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testLz4 test_lz4io.cpp -std=c++0x

testintermediate:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testIntermediate test_intermediatetripledata.cpp -std=c++0x

testnewcompress:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testNewcompress test_newcompress.cpp -std=c++0x

testreaddict:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testReadDict test_readdictfiles.cpp -std=c++0x

testsizegroups:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testsizegroups -llz4 test_sizegroups.cpp -std=c++0x

testloadmap:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testLoadMap -llz4 test_loadmap.cpp -std=c++0x

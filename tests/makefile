KOGNAC_BUILDIR=../../kognac/build_debug
TRIDENT_BUILDIR=../build_debug
CINCLUDES= -I../include -I../../kognac/include -I../snap/snap-core -I../snap/glib-core -I../build/kognac/external/lz4/lib -I../build/kognac/external/sparsehash/src
CLIBS=-L$(KOGNAC_BUILDIR) -L$(TRIDENT_BUILDIR) -L../snap/snap-core -L../build/kognac/external/lz4/lib -L../build/kognac/external/sparsehash/src
CPLUS=g++

test_httpclient:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testHttpClient -std=c++0x -O0 -g test_httpclient.cpp ../src/trident/utils/httpclient.cpp ../../kognac/src/kognac/log/logs.cpp -lkognac

test_httpserver:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testHttp -std=c++0x -O0 -g test_httpserver.cpp ../src/trident/utils/httpserver.cpp ../../kognac/src/kognac/log/logs.cpp

test_multi:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testMulti -std=c++0x -DBOOST_LOG_DYN_LINK  -O0 -g test_multi.cpp $(OBJS) -ltrident -lkognac -lpthread  -ltbb -lboost_filesystem -lboost_system -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options -lboost_iostreams -llz4 -lsnap

test_transe4:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testTranse4 -std=c++0x -DBOOST_LOG_DYN_LINK -g -O0 test_transe4.cpp $(OBJS) -ltrident -lkognac -lpthread  -ltbb -lboost_filesystem -lboost_system -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options -lboost_iostreams -llz4 -lsnap -lboost_serialization

test_transe3:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testTranse3 -std=c++0x -DBOOST_LOG_DYN_LINK -g -O0 test_transe3.cpp $(OBJS) -ltrident -lkognac -lpthread  -ltbb -lboost_filesystem -lboost_system -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options -lboost_iostreams -llz4 -lsnap -lboost_serialization

test_transe2:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testTranse2 -std=c++0x -DBOOST_LOG_DYN_LINK -g -O0 test_transe2.cpp $(OBJS) -ltrident -lkognac -lpthread  -ltbb -lboost_filesystem -lboost_system -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options -lboost_iostreams -llz4 -lsnap -lboost_serialization

test_transe:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testTranse -std=c++0x -DBOOST_LOG_DYN_LINK -g -O0 test_transe.cpp $(OBJS) -ltrident -lkognac -lpthread  -ltbb -lboost_filesystem -lboost_system -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options -lboost_iostreams -llz4 -lsnap

test_snap:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testSnap -std=c++0x -DBOOST_LOG_DYN_LINK  -O3 test_snap.cpp $(OBJS) -ltrident -lpthread  -ltbb -lboost_filesystem -lboost_system -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options -lboost_iostreams -llz4 -lsnap ../..//kognac/build/libkognac.a

test_insert5:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o ./testInsert5 -std=c++0x -DBOOST_LOG_DYN_LINK  -O0 test_insert5.cpp $(OBJS) -ltrident -lkognac -lpthread  -ltbb -lboost_filesystem -lboost_system -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options -lboost_iostreams -llz4

test_treeitr:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o treeItr -std=c++0x -DBOOST_LOG_DYN_LINK  -O0 test_treeitr.cpp $(OBJS) -ltrident -lkognac -lpthread  -ltbb -lboost_filesystem -lboost_system -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options -lboost_iostreams -llz4

test_insertlarge:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o testInsertlarge -DBOOST_LOG_DYN_LINK  -O3 test_insertlarge.cpp $(OBJS) -ltrident -lkognac -lpthread  -ltbb -lboost_filesystem -lboost_system -lboost_chrono -lboost_thread -lboost_log -lboost_log_setup -lboost_program_options -lboost_iostreams -llz4

test_graph:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -I../snap/snap-core -I../snap/glib-core -L../snap/snap-core -o testGraph -O3 test_graph.cpp -lsnap

test_generate:
	g++ $(CINCLUDES) $(CLIBS) -o testGenerate -DBOOST_LOG_DYN_LINK  -O0 -g test_generate.cpp -lboost_log-mt -lboost_log_setup-mt -llz4 -lboost_filesystem-mt -lboost_system-mt -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread  -ltbb -ltrident -lkognac

test_sorting2:
	g++ $(CINCLUDES) $(CLIBS) -o testSorting2 -DBOOST_LOG_DYN_LINK  -O0 -g test_sorting2.cpp -lkognac -lboost_log-mt -lboost_log_setup-mt -llz4 -lboost_filesystem-mt -lboost_system-mt -lboost_chrono-mt -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread  -ltbb

test_pfor:
	g++ $(CINCLUDES) $(CLIBS) -o testpfor -O3 -std=c++0x -g -lkognac-log -lkognac -I../include test_parallel_for.cpp 

test_sorting3:
	g++ $(CINCLUDES) $(CLIBS) -o testSorting3 -O3 -std=c++0x -g -I../include test_sorting3.cpp 
test_io:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -lboost_log-mt -lboost_log_setup-mt -llz4 -lboost_filesystem-mt -lboost_system -lboost_chrono -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread -o test_io -DBOOST_LOG_DYN_LINK  -O3 test_io.cpp -std=c++0x $(KOGNAC_BUILDIR)/kognac/compression/*.o $(KOGNAC_BUILDIR)/kognac/schema/*.o $(KOGNAC_BUILDIR)/kognac/sorting/*.o $(KOGNAC_BUILDIR)/kognac/utils/*.o

test_reasoner:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -lboost_log-mt -lboost_log_setup-mt -llz4 -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread -o test -O0 test_reasoner.cpp -std=c++0x -DBOOST_LOG_DYN_LINK ../build/tree/*.o ../build/utils/*.o ../build/files/*.o ../build/kb/*.o ../build/storage/*.o ../build/iterators/*.o ../build/reasoner/*.o

testinsertperm:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -o testIndexPerm -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -llz4 -lpthread test_insert_perm.cpp -std=c++0x  $(KOGNAC_BUILDIR)/**/*.o  $(TRIDENT_BUILDIR)/**/*.o

testquerytree:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o testTree -O3 -llz4 -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread -lstxxl test_querytree.cpp -std=c++0x ../build/tree/*.o ../build/utils/*.o ../build/files/*.o ../build/kb/*.o ../build/storage/*.o ../build/iterators/*.o

testdictionary:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -o testDictionary -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread -lstxxl test_dictionary_tree.cpp -std=c++0x ../build/tree/*.o ../build/utils/*.o ../build/lz4/*.o ../build/files/*.o

testcache:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -I../ -lboost_system -lboost_chrono -o testCache -std=c++0x test_cache.cpp

testnumbers:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -I../ -o testNumber -DBOOST_LOG_DYN_LINK  -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread -llz4 test_number.cpp ../tridentcompr/utils/utils.cpp -std=c++0x

testindex:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -o testIndex -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread -lstxxl test_index.cpp -std=c++0x ../build/utils/*.o ../build/tree/*.o ../build/lz4/*.o ../build/files/*.o

testmergesort:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O3 -o testMerge -DBOOST_LOG_DYN_LINK -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread -lstxxl test_mergesort.cpp -std=c++0x ../build/compression/*.o ../build/sorting/*.o ../build/utils/*.o ../build/tree/*.o ../build/lz4/*.o ../build/files/*.o ../build/main/loader.o ../build/kb/*.o ../build/storage/*.o ../build/sparql/*.o ../build/iterators/*.o

OBJC=$(wildcard ../../kognac/build/kognac/**/*.o)
testinsertdictionary:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -I.. -I../include -O3 -g -o testInsertDictionary -DBOOST_LOG_DYN_LINK -lboost_filesystem-mt -llz4 -lboost_system -lboost_chrono -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread test_insertdictionary.cpp -std=c++0x $(OBJS) $(OBJC)

testinserttree:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -I.. -I../include -O3 -g -o testInsertTree -llz4 -lz -ltbb -lpthread test_insert_tree.cpp -std=c++0x $(OBJS) $(OBJC) ../build_debug/libtrident.a ../build_debug/kognac/libkognac.a

testinsertindex:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -I.. -I../include -I../../kognac/include -O3 -g -o testInsertIndex -DBOOST_LOG_DYN_LINK -ltcmalloc -lboost_filesystem-mt -llz4 -lboost_system -lboost_chrono -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt -lpthread test_insertindex.cpp -std=c++0x $(OBJS) $(OBJC)

testlz4:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testLz4 -DBOOST_LOG_DYN_LINK -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt test_lz4io.cpp -std=c++0x ../build_debug/compression/*.o ../build_debug/sorting/*.o ../build_debug/utils/*.o ../build_debug/tree/*.o ../build_debug/lz4/*.o ../build_debug/files/*.o ../build_debug/main/loader.o ../build_debug/kb/*.o ../build_debug/storage/*.o ../build_debug/sparql/*.o ../build_debug/iterators/*.o

testintermediate:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testIntermediate -DBOOST_LOG_DYN_LINK -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt test_intermediatetripledata.cpp -std=c++0x ../build_debug/compression/*.o ../build_debug/sorting/*.o ../build_debug/utils/*.o ../build_debug/tree/*.o ../build_debug/lz4/*.o ../build_debug/files/*.o ../build_debug/main/loader.o ../build_debug/kb/*.o ../build_debug/storage/*.o ../build_debug/sparql/*.o ../build_debug/iterators/*.o

testnewcompress:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testNewcompress -DBOOST_LOG_DYN_LINK -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt test_newcompress.cpp -std=c++0x ../build_debug/compression/*.o ../build_debug/sorting/*.o ../build_debug/utils/*.o ../build_debug/tree/*.o ../build_debug/lz4/*.o ../build_debug/files/*.o ../build_debug/main/loader.o ../build_debug/kb/*.o ../build_debug/storage/*.o ../build_debug/sparql/*.o ../build_debug/iterators/*.o

testreaddict:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testReadDict -DBOOST_LOG_DYN_LINK -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt test_readdictfiles.cpp -std=c++0x ../build_debug/compression/*.o ../build_debug/sorting/*.o ../build_debug/utils/*.o ../build_debug/tree/*.o ../build_debug/lz4/*.o ../build_debug/files/*.o ../build_debug/main/loader.o ../build_debug/kb/*.o ../build_debug/storage/*.o ../build_debug/sparql/*.o ../build_debug/iterators/*.o

testsizegroups:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testsizegroups -DBOOST_LOG_DYN_LINK -lboost_filesystem-mt -lboost_system -lboost_chrono -llz4 -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt test_sizegroups.cpp -std=c++0x ../build_debug/compression/*.o ../build_debug/schema/*.o ../build_debug/sorting/*.o ../build_debug/utils/*.o ../build_debug/tree/*.o ../build_debug/files/*.o ../build_debug/main/loader.o ../build_debug/kb/*.o ../build_debug/storage/*.o ../build_debug/sparql/*.o ../build_debug/iterators/*.o

testloadmap:
	$(CPLUS) $(CINCLUDES) $(CLIBS) -O0 -g -o testLoadMap -DBOOST_LOG_DYN_LINK -llz4 -lboost_filesystem-mt -lboost_system -lboost_chrono -lraptor2 -lrasqal -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options-mt -lboost_iostreams-mt test_loadmap.cpp -std=c++0x ../build_debug/compression/*.o ../build_debug/sorting/*.o ../build_debug/utils/*.o ../build_debug/tree/*.o ../build_debug/files/*.o ../build_debug/main/loader.o ../build_debug/kb/*.o ../build_debug/storage/*.o ../build_debug/sparql/*.o ../build_debug/iterators/*.o ../build_debug/schema/*.o

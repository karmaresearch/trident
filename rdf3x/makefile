#Include variables dependant on the platform
OUTPUTDIR=.
CPLUS=g++

LDFLAGS=-DBOOST_LOG_DYN_LINK -lboost_chrono -lboost_log-mt -lboost_log_setup-mt
CPPFLAGS=-DBOOST_LOG_DYN_LINK -I include/ -c -std=c++11 -O3
CFLAGS= -c -std=c99

SRCDIR=src
O_FILES=$(wildcard $(SRCDIR)/*.cpp) $(wildcard $(SRCDIR)/**/*.cpp) $(wildcard $(SRCDIR)/**/**/*.cpp)
BFILES=$(subst $(SRCDIR),$(BUILDIR),$(O_FILES:.cpp=.o))

BUILDIR=$(OUTPUTDIR)/build
RDF3X=$(BUILDIR_RELEASE)/rdf3x

$(RDF3X): init $(BFILES)
	$(CPLUS) -o $@ $(BFILES) $(LDFLAGS) 

init:
	@mkdir -p $(BUILDIR)	

$(BUILDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p `dirname $@`
	$(CPLUS) $(CPPFLAGS) $< -o $@ 

.PHONY: clean
clean:
	@rm -rf $(BUILDIR)
	@echo "Cleaning completed"

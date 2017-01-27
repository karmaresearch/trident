#
# Makefile for non-Microsoft compilers
#

all: MakeAll

test: TestAll

MakeAll:
	$(MAKE) -C snap-core

TestAll:
	$(MAKE) run -C test

clean:
	$(MAKE) clean -C snap-core

CXX = g++
ifeq ($(__REL), 1)
  CXXFLAGS = -O3 -g
else
  CXXFLAGS = -O0 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -gdwarf-2 -Wno-unused-variable
endif

OBJECT = mongosync
OUTPUT = ./output
DRIVER_DIR = ./dep/mongo-cxx-driver
DRIVER_LIB = $(DRIVER_DIR)/build/install/lib/libmongoclient.a
VERSION = -D_GITVER_=$(shell git rev-list HEAD | head -n1) \
					-D_COMPILEDATE_=$(shell date +%FT%T%z)

CPU_NUM = $(shell cat /proc/cpuinfo | grep CPU | wc -l)
ifneq ($(CPU_NUM), 1)
CPU_USE=$(shell expr $(CPU_NUM) / 2)
else
CPU_USE=$(CPU_NUM)
endif

LIB_PATH = -L$(DRIVER_DIR)/build/install/lib/ \
					 -L/usr/local/lib/ \
					 -L/usr/lib64/

LIBS = -lm \
			 -lpthread \
			 -lboost_regex-mt \
			 -lboost_thread-mt \
			 -lboost_system-mt \
			 -lrt \
			 -lssl \
			 -lcrypto \

INCLUDE_PATH = -I$(DRIVER_DIR)/build/install/include/

.PHONY: all driver distclean clean 


BASE_BOJS := $(wildcard ./*.cc)
OBJS = $(patsubst %.cc,%.o,$(BASE_BOJS))

all: $(OBJECT)
	rm -rf $(OUTPUT)
	mkdir -p $(OUTPUT)
	mkdir -p $(OUTPUT)
	cp $(OBJECT) $(OUTPUT)
	rm -rf $(OBJECT)
	cp mongosync.conf $(OUTPUT)
	@echo "Success, go, go, go..."


mongosync: $(DRIVER_LIB) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(DRIVER_LIB) $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

driver:
	scons -C $(DRIVER_DIR) --ssl install -j $(CPU_USE)

$(DRIVER_LIB):
	scons -C $(DRIVER_DIR) --ssl install -j $(CPU_USE)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) $(VERSION)

distclean: clean
	scons -C $(DRIVER_DIR) --ssl -c .

clean: 
	rm -rf $(OUTPUT)
	rm -f ./*.o
	rm -rf $(OBJECT)



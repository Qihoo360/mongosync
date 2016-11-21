CXX = g++
#CXXFLAGS = -O0 -g
CXXFLAGS = -O0 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -gdwarf-2 -Wno-unused-variable

OBJECT = mongosync
OUTPUT = ./output
DRIVER_DIR = ./dep/mongo-cxx-driver-legacy-1.0.0
DRIVER_LIB = $(DRIVER_DIR)/build/install/lib/libmongoclient.a

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

.PHONY: all dist_clean clean version


BASE_BOJS := $(wildcard ./*.cc)
OBJS = $(patsubst %.cc,%.o,$(BASE_BOJS))

all: $(OBJECT)
	rm -rf $(OUTPUT)
	mkdir -p $(OUTPUT)
	mkdir -p $(OUTPUT)
	cp $(OBJECT) $(OUTPUT)
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."


mongosync: $(DRIVER_LIB) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(DRIVER_LIB) $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(DRIVER_LIB):
	scons -C $(DRIVER_DIR) --ssl install -j $(CPU_USE)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

dist_clean: clean
	scons -C $(DRIVER_DIR) --ssl -c .

clean: 
	rm -rf $(OUTPUT)
	rm -f ./*.o
	rm -rf $(OBJECT)



CXX=g++
CXXFLAGS=-std=c++0x -g -Wall
INC=-I.. -I. -I../rpc -I../base
LIB=-Llib -lrpc -lbase -lpthread

.PHONY: all
all: clean bin/server bin/client bin/client_share_n bin/client_share_one

.PHONY: clean
clean:
	rm -f counter.h
	rm -rf lib
	rm -rf bin
	rm -f *.o
	mkdir bin
	mkdir lib

counter.h: ../bin/rpcgen counter.rpc
	../bin/rpcgen counter.rpc

bin/server: s.cc counter.h lib/librpc.a lib/libbase.a
	${CXX} ${CXXFLAGS} ${INC} s.cc ${LIB} -o bin/server

bin/client: c.cc counter.h lib/librpc.a lib/libbase.a
	${CXX} ${CXXFLAGS} ${INC} c.cc ${LIB} -o bin/client

bin/client_share_n: c_share_n.cc counter.h lib/librpc.a lib/libbase.a
	${CXX} ${CXXFLAGS} ${INC} c_share_n.cc ${LIB} -o bin/client_share_n

bin/client_share_one: c_share_one.cc counter.h lib/librpc.a lib/libbase.a
	${CXX} ${CXXFLAGS} ${INC} c_share_one.cc ${LIB} -o bin/client_share_one

lib/librpc.a: client.o marshal.o polling.o server.o utils.o
	ar rcs lib/librpc.a client.o marshal.o polling.o server.o utils.o
	rm -f client.o marshal.o polling.o server.o utils.o

lib/libbase.a: basetypes.o logging.o misc.o strop.o threading.o unittest.o
	ar rcs lib/libbase.a basetypes.o logging.o misc.o strop.o threading.o unittest.o
	rm -f basetypes.o logging.o misc.o strop.o threading.o unittest.o

client.o: ../rpc/client.cc ../rpc/client.h
	${CXX} ${CXXFLAGS} ${INC} -c ../rpc/client.cc -o client.o

marshal.o: ../rpc/marshal.cc ../rpc/marshal.h
	${CXX} ${CXXFLAGS} ${INC} -c ../rpc/marshal.cc -o marshal.o

polling.o: ../rpc/polling.cc ../rpc/polling.h
	${CXX} ${CXXFLAGS} ${INC} -c ../rpc/polling.cc -o polling.o

server.o: ../rpc/server.cc ../rpc/server.h
	${CXX} ${CXXFLAGS} ${INC} -c ../rpc/server.cc -o server.o

utils.o: ../rpc/utils.cc ../rpc/utils.h
	${CXX} ${CXXFLAGS} ${INC} -c ../rpc/utils.cc -o utils.o

basetypes.o: ../base/basetypes.cc ../base/basetypes.h
	${CXX} ${CXXFLAGS} ${INC} -c ../base/basetypes.cc -o basetypes.o

logging.o: ../base/logging.cc ../base/logging.h
	${CXX} ${CXXFLAGS} ${INC} -c ../base/logging.cc -o logging.o

misc.o: ../base/misc.cc ../base/misc.h
	${CXX} ${CXXFLAGS} ${INC} -c ../base/misc.cc -o misc.o

strop.o: ../base/strop.cc ../base/strop.h
	${CXX} ${CXXFLAGS} ${INC} -c ../base/strop.cc -o strop.o

threading.o: ../base/threading.cc ../base/threading.h
	${CXX} ${CXXFLAGS} ${INC} -c ../base/threading.cc -o threading.o

unittest.o: ../base/unittest.cc ../base/unittest.h
	${CXX} ${CXXFLAGS} ${INC} -c ../base/unittest.cc -o unittest.o


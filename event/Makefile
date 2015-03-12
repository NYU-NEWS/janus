test: task.hpp scheduler.hpp scheduler.cpp event.hpp test.cpp
	g++ -std=c++11 scheduler.hpp task.hpp scheduler.cpp event.hpp test.cpp -o task -lboost_coroutine -lboost_context

mutex: mutex.cpp
	g++ -std=c++11 mutex.cpp -o mutex -lboost_coroutine -lboost_context

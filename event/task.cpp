#include <boost/coroutine/all.hpp>
#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <mutex>

#include "scheduler.hpp"

typedef boost::coroutines::coroutine< void()> coro_t;

class Scheduler;

class Event{
public:
	Event(void* t):T(t){

	}
	void* T;
};

class Task{
	Scheduler* scheduler;
	std::string name;
public:
	Task(std::string n, Scheduler* s=NULL): name(n), scheduler(s){
		
	}
	~Task(){
	}

	void run(coro_t::caller_type &ca){
		scheduler->get_lock(this);
		std::cout << name << " gets the lock\n";
		scheduler->release_lock(this);
		std::cout << name << " releases the lock\n";
	}
};

int main(){
	Scheduler* scheduler = new Scheduler();
	Task* task1 = new Task(std::string("task1"));
	Task* task2 = new Task(std::string("task2"));

	scheduler->add_task(task1);
	scheduler->add_task(task2); 
}

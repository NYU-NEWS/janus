#include <boost/coroutine/all.hpp>
#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <mutex>

#include "scheduler.hpp"
#include "event.hpp"

typedef boost::coroutines::coroutine< void()> coro_t;


class Task{
	Scheduler* scheduler;
	std::string name;
	coro_t* cur_coro;

public:
	Task(std::string n, Scheduler* s=NULL): name(n), scheduler(s){
		
	}
	~Task(){
	}

	void get_lock(){
		while (scheduler->get_lock(new Event(this))){
			std::cout << name << " gets the lock\n"; 
		}
	}
	void unlock(){
		scheduler->release_lock();
	}

	void lock_and_unlock(){
		cur_coro = new coro_t(run);
		(*cur_coro)();
	}

	void run(coro_t::caller_type &ca){
		if ( scheduler->get_lock(new Event(this)) ){
			std::cout << name << " gets the lock\n";	
		}else{
			ca();
		}
		
		scheduler->release_lock();
		std::cout << name << " releases the lock\n";
	}
};


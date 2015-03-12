/*************************************************************************
 > File Name: schedular.hpp
 > Author: Mengxing Liu
*************************************************************************/

#ifndef SCHEDULER_HPP
#define SCHEDULER_HPP

#include <boost/coroutine/all.hpp>
#include "event.hpp"

class Task;
class Event;

typedef boost::coroutines::coroutine<void()> coro_t;

class Scheduler{
	std::vector<Task*> task_queue;
	std::map<std::mutex, std::set<Event*> > event_map;

	std::mutex mtx;
	int count;
public:
	Scheduler(){
		count = 1;
	}
	void add_task(Task* const task){
		task_queue.insert(task);	
	}

	bool get_lock(Event* const ev){
		if (count == 1){
			mtx.try_lock();
			return true;
		}else{
			event_map[mtx].insert(ev);
			return false;
		}
	}
	void release_lock(Task* task){
		mtx.unlock();
		if (event_map[mtx].size() > 0){
			Event* ev = *(event_map[mtx].begin());
			ev->trigger();
			(*ev->coro)();
		}
	}

	void run();

};
#endif

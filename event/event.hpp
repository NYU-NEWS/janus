#ifndef EVENT_HPP
#define EVENT_HPP

#include "task.hpp"

class Event{
	coro_t* coro;
	bool triggered;
public:
	Event(Task* task): coro(task->cur_coro), triggered(false){

	}
	void trigger(){
		this.triggered = true;
	}

	bool status(){
		return triggered;
	}
};

#endif
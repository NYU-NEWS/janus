/*************************************************************************
 > File Name: schedular.h
 > Author: Mengxing Liu
*************************************************************************/

#ifndef SCHEDULER_H
#define SCHEDULER_H

class Task;
class Event;

class Scheduler{
	std::vector<Task*> task_queue;
	std::map<Event, std::vector<Task*> > task_map;

	std::mutex mtx;
	int count;
public:
	Scheduler(){
		count = 1;
	}
	void add_task(Task* const task){
		task_queue.push_back(task);	
	}

	void get_lock(Task* const task){
		if (count == 1){
			mtx.try_lock();
		}else{
		}
	}
	void release_lock(Task* task){
		mtx.unlock();
	}

};
#endif

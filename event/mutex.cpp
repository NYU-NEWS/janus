/*************************************************************************
 > File Name: mutex.cpp
 > Author: Mengxing Liu
 ************************************************************************/

#include <iostream>
#include <mutex>
#include <boost/coroutine/coroutine.hpp>
#include <map>
#include <set>

typedef boost::coroutines::coroutine< void(void *)> coro_t;

int count;
std::mutex mtx;

class event{
	coro_t* coro;
public:
	event(coro_t* c):coro(c){

	}
	void trigger(){
		(*coro)(NULL);
	}
};

std::map<std::mutex*, std::set<event*> > event_map;

event* mkevent(coro_t* cur_coro){
	return new event(cur_coro);
}

bool trylock(){
	if(count < 1){
		return false;
	}else{
		mtx.try_lock();
		count--;
		return true;
	}
}
void unlock(){
	if (count == 0){
		count++;
		mtx.unlock();
		if (event_map[&mtx].size()>0){
			event* ev = (event*)*(event_map[&mtx].begin());
			event_map[&mtx].erase(ev);

			ev->trigger();
		}
	}
}

void txn2(coro_t::caller_type& ca){
	ca.get();
	ca();

	coro_t* cur_coro = (coro_t*)(ca.get());
	event* lock_ev = mkevent(cur_coro);
	
	while(trylock() == false){
		event_map[&mtx].insert(lock_ev);
		cur_coro = NULL;
		ca();
	}

	std::cout << "task2 is doing stuff\n";
	
	// do something else

	std::cout << "task2 unlock\n";
	unlock();
}

void txn1(coro_t::caller_type& ca){
	ca.get();
	ca();

	coro_t* cur_coro = (coro_t*)(ca.get());
	event* lock_ev = mkevent(cur_coro);

	while(trylock() == false){
		event_map[&mtx].insert(lock_ev);
		cur_coro = NULL;
		ca();
	}	
	std::cout << "task1 is doing stuff\n";

	// may yield because of waiting for I/O or Internet message
	ca();	
	
	std::cout << "task1 unlock\n";
	unlock();
}

int main(){
	count = 1;

	coro_t c1(txn1, NULL);
	c1((void*) &c1);
	
	coro_t c2(txn2, NULL);
	c2((void*) &c2);

	// txn1 waiting condition is trigger
	c1(NULL);
}


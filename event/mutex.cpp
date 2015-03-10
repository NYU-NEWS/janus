/*************************************************************************
 > File Name: mutex.cpp
 > Author: Mengxing Liu
 ************************************************************************/

#include <iostream>
#include <mutex>
#include <boost/coroutine/coroutine.hpp>

typedef boost::coroutines::coroutine< void(void)> coro_t;

int count;
std::mutex mtx;

void lock_and_unlock(coro_t::caller_type& ca){
	while(count < 1){
		// count != 0 then break the function and return back
		ca();
	}
	mtx.try_lock();
	std::cout << "task2 is doing stuff\n";
	mtx.unlock();
	std::cout << "task2 unlock\n";
}

void tx1_begin(){
	if (mtx.try_lock()){
		count--;
		std::cout << "task1 is doing stuff\n";
	}
}

void tx1_end(coro_t* c){
	mtx.unlock();
	count++;
	std::cout << "task1 unlock\n";
	(*c)();
}

int main(){
	count = 1;
	tx1_begin();
	coro_t c(lock_and_unlock);
	c();
	tx1_end(&c);
}


#pragma once

#include <stdio.h>
#include <inttypes.h>

#include <string>

#include "basetypes.hpp"

namespace rrr {

inline uint64_t rdtsc() {
    uint32_t hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return (((uint64_t) hi) << 32) | ((uint64_t) lo);
}

template<class T, class T1, class T2>
inline T clamp(const T& v, const T1& lower, const T2& upper) {
    if (v < lower) {
        return lower;
    }
    if (v > upper) {
        return upper;
    }
    return v;
}

// YYYY-MM-DD HH:MM:SS.mmm, 24 bytes required for now
#define TIME_NOW_STR_SIZE 24
void time_now_str(char* now);

int get_ncpu();

const char* get_exec_path();

// NOTE: \n is stripped from input
std::string getline(FILE* fp, char delim = '\n');

// This template function declaration is used in defining arraysize.
// Note that the function doesn't need an implementation, as we only
// use its type.
template <typename T, size_t N>
char (&ArraySizeHelper(T (&array)[N]))[N];

// That gcc wants both of these prototypes seems mysterious. VC, for
// its part, can't decide which to use (another mystery). Matching of
// template overloads: the final frontier.
#ifndef COMPILER_MSVC
template <typename T, size_t N>
char (&ArraySizeHelper(const T (&array)[N]))[N];
#endif

#define arraysize(array) (sizeof(base::ArraySizeHelper(array)))

template <class K, class V, class Map>
inline void insert_into_map(Map& map, const K& key, const V& value) {
    map.insert(typename Map::value_type(key, value));
}

template<class Container>
typename std::reverse_iterator<typename Container::iterator>
erase(Container &l,
        typename std::reverse_iterator<typename Container::iterator> &rit) {
    typename Container::iterator it = rit.base();
    it--;
    it = l.erase(it);
    return std::reverse_iterator<typename Container::iterator>(it);
}

class FrequentJob {
public:
    uint64_t tm_last_ = 0;
    uint64_t period_ = 0;

    virtual ~FrequentJob() {}
    virtual void run() = 0;
    
    virtual void trigger() {
        uint64_t tm_now = rrr::Time::now();
        uint64_t s = tm_now - tm_last_;

        if (s > period_) {
            tm_last_ = tm_now;
            run();
	    }
    }

    virtual uint64_t get_last_time() {
        return tm_last_;
    }

    virtual void set_period(uint64_t p) {
        period_ = p;
    }
};

} // namespace base

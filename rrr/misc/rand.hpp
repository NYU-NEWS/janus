#ifndef RANDOM_GENERATOR_H_
#define RANDOM_GENERATOR_H_

#include <pthread.h>

namespace rrr {

class RandomGenerator {
private:
#if defined(__APPLE__) || defined(__clang__)
    static pthread_key_t seed_key_;
    static pthread_once_t seed_key_once_;
    static pthread_once_t delete_key_once_;

    static void create_key();
    static void delete_key();

    static unsigned int *get_seed();
#else // not __APPLE__
    static thread_local unsigned int seed_;
#endif // __APPLE__

    static int nu_constant;
    static unsigned long long rdtsc();

public:

    static int rand(int min = 0, int max = RAND_MAX); // range: [min, max]

    static double rand_double(double min = 0.0, 
			      double max = (double)RAND_MAX); // range: [min, max]

    static std::string rand_str(int length = 0);

    static std::string int2str_n(int i, int length);

    static int nu_rand(int a, int x, int y);

    static bool percentage_true(double p);

    static bool percentage_true(int p);

    static unsigned int weighted_select(const std::vector<double> &weight_vector);

    static void destroy();
};

}

#endif

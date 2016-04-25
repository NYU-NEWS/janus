#pragma once
#include <math.h>
#include "deptran/__dep__.h"

namespace rococo {

class ZipfDist {
 public:
  struct probvals {
    float prob;
    /* the access probability */
    float cum_prob;              /* the cumulative access probability */
  };
  vector<probvals> zdist = {};

  void get_zipf(float alpha, int N) {
    float theta;
    float sum = 0.0;
    float c = 0.0;
    float sumc = 0.0;
    int i;

    /*
     * pmf: f(x) = 1 / (x^alpha) * sum_1_to_n( (1/i)^alpha )
     */

    for (i = 1; i <= N; i++) {
      sum += 1.0 / (float) pow((double) i, (double) (alpha));

    }
    c = 1.0 / sum;

    for (i = 0; i < N; i++) {
      zdist[i].prob = c /
          (float) pow((double) (i + 1), (double) (alpha));
      sumc += zdist[i].prob;
      zdist[i].cum_prob = sumc;
    }
  }

  ZipfDist(float alpha, int N) {
    int i;

    if (N <= 0 || alpha < 0.0 || alpha > 1.0) {
      Log_fatal("wrong arguments for zipf");
    }

    zdist.resize(N);
    get_zipf(alpha, N);          /* generate the distribution */
  }

  int operator() (boost::random::mt19937& rand_gen) {
    auto x = rand_gen();
    auto xx = (x - rand_gen.min()) / (double)(rand_gen.max() - rand_gen.min());
    for (int i = 0; i < zdist.size(); i++) {
      auto& z = zdist[i];
      if (xx < z.cum_prob)
        return i;
    }
  }
};

} // namespace rococo

     



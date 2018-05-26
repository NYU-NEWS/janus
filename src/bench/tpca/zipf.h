#pragma once
#include <random>
#include <math.h>
#include "deptran/__dep__.h"

namespace janus {

class ZipfDist {
 public:
  struct probvals {
    double prob;
    /* the access probability */
    double cum_prob;              /* the cumulative access probability */
  };
  vector<probvals> zdist = {};

  void get_zipf(double alpha, int N) {
    double theta;
    double sum = 0.0;
    double c = 0.0;
    double sumc = 0.0;
    int i;

    /*
     * pmf: f(x) = 1 / (x^alpha) * sum_1_to_n( (1/i)^alpha )
     */

    for (i = 1; i <= N; i++) {
      sum += 1.0 / (double) pow((double) i, (double) (alpha));

    }
    c = 1.0 / sum;

    for (i = 0; i < N; i++) {
      zdist[i].prob = c /
          (double) pow((double) (i + 1), (double) (alpha));
      sumc += zdist[i].prob;
      zdist[i].cum_prob = sumc;
    }
    zdist[N-1].cum_prob = 1.0;
  }

  ZipfDist(double alpha, int N) {
    int i;

    if (N <= 0 || alpha < 0.0 || alpha > 1.0) {
      Log_fatal("wrong arguments for zipf");
    }

    zdist.resize(N);
    get_zipf(alpha, N);          /* generate the distribution */
  }

  int operator() (std::mt19937& rand_gen) {
//    for (int i = 0; i < zdist.size()-1; i++) {
//      auto& z1 = zdist[i];
//      auto& z2 = zdist[i+1];
//      verify(z1.cum_prob < z2.cum_prob);
//    }

    auto x = rand_gen();
    double xx = (x - rand_gen.min()) /
        (double)(rand_gen.max() - rand_gen.min());
    verify(xx >= 0 && xx <= 1);
    int start_search = 0;
    int end_search = zdist.size() - 1;
    verify(end_search >= start_search);
    int middle = 0;
    while (start_search <= end_search) {
      auto& zz_start = zdist[start_search];
      auto& zz_end = zdist[end_search];
//      double d99997 = zdist[99998].cum_prob;
//      double d99998 = zdist[99998].cum_prob;
//      double d99999 = zdist[99999].cum_prob;
      verify(zz_end.cum_prob >= xx);

      if (start_search == end_search) {
        verify(zdist[start_search].cum_prob >= xx);
        return start_search;
      }
      middle = (start_search + end_search) / 2;
//      if (middle == start_search) {
//        auto& z = zdist[middle];
////        verify(z.cum_prob);
//        if (middle > 0) {
//          verify(zdist[middle-1].cum_prob <= xx);
//        }
//        return middle;
//      }
      verify(middle >= start_search);
      verify(middle <= end_search);
      if (middle == 0) {
        return middle;
      }
      verify(middle > 0);
      auto& z = zdist[middle-1];
      auto& zz = zdist[middle];
      if (xx > z.cum_prob && xx <= zz.cum_prob) {
        return middle;
      } else if (xx < zz.cum_prob) {
        end_search = middle - 1;
      } else if (xx > zz.cum_prob) {
        start_search = middle + 1;
      } else {
        verify(0);
      }
    }
    verify(0);
  }
};

} // namespace janus

     



#include "gtest/gtest.h"

TEST(FailTest, ShouldFail) {
  EXPECT_EQ(1, 0);
}
TEST(PassTest, ShouldPass) {
  EXPECT_EQ(1, 1);
}

// Step 3. Call RUN_ALL_TESTS() in main().

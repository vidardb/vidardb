//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "db/write_controller.h"

#include "vidardb/env.h"
#include "util/testharness.h"

namespace vidardb {

class WriteControllerTest : public testing::Test {};

class TimeSetEnv : public EnvWrapper {
 public:
  explicit TimeSetEnv() : EnvWrapper(nullptr) {}
  uint64_t now_micros_ = 6666;
  virtual uint64_t NowMicros() override { return now_micros_; }
};

TEST_F(WriteControllerTest, SanityTest) {
  WriteController controller(10000000u);
  auto stop_token_1 = controller.GetStopToken();
  auto stop_token_2 = controller.GetStopToken();

  ASSERT_TRUE(controller.IsStopped());
  stop_token_1.reset();
  ASSERT_TRUE(controller.IsStopped());
  stop_token_2.reset();
  ASSERT_FALSE(controller.IsStopped());
}

}  // namespace vidardb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

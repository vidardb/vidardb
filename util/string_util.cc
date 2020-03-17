//  Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "util/string_util.h"

#include <sstream>
#include <string>
#include <vector>

namespace vidardb {

std::vector<std::string> StringSplit(const std::string& arg, char delim) {
  std::vector<std::string> splits;
  std::stringstream ss(arg);
  std::string item;
  while (std::getline(ss, item, delim)) {
    splits.push_back(item);
  }
  return splits;
}

std::string StringStitch(const std::vector<std::string>& args, char delim) {
  std::string result;
  for (auto i = 0u; i < args.size(); i++) {
    result.append(args[i]);
    if (i < args.size() - 1) {
      result.append(1, delim);
    }
  }
  return result;
}

}  // namespace vidardb

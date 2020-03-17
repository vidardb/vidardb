//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include <string>

#include "vidardb/env.h"
#include "vidardb/options.h"
#include "vidardb/status.h"
#include "vidardb/types.h"

namespace vidardb {

extern Status CopyFile(Env* env, const std::string& source,
                       const std::string& destination, uint64_t size = 0);

extern Status CreateFile(Env* env, const std::string& destination,
                         const std::string& contents);

extern Status DeleteSSTFile(const DBOptions* db_options,
                            const std::string& fname, uint32_t path_id);

}  // namespace vidardb

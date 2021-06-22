//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
//  For efficiently handling range queries, we directly load the data blocks
//  (not index blocks) in specified memory (for pgrocks_am, it is shared memory),
//  and then iterate over attributes without any decoding.
//
//  The data blocks are loaded from the beginning to the end in the specified
//  memory, while the attribute headers describing the offset and size of an
//  attribute are written, with the parsing of attributes, from the end to the
//  beginning in the specified memory.
//
#pragma once

#include <stdint.h>

namespace vidardb {
// reserved for future data structures about the specified memory area
}  // namespace vidardb

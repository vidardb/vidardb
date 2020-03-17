//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "vidardb/iterator.h"
#include "vidardb/status.h"

namespace vidardb {

class InternalIterator;

// Seek to the properties block.
// If it successfully seeks to the properties block, "is_found" will be
// set to true.
Status SeekToPropertiesBlock(InternalIterator* meta_iter, bool* is_found);

// Seek to the compression dictionary block.
// If it successfully seeks to the properties block, "is_found" will be
// set to true.
Status SeekToCompressionDictBlock(InternalIterator* meta_iter, bool* is_found);

/****************************** Shichao *****************************/
// Seek to the column block.
// Return true if it successfully seeks to that block.
Status SeekToColumnBlock(InternalIterator* meta_iter, bool* is_found);
/****************************** Shichao *****************************/
}  // namespace vidardb

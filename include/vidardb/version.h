// Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#define VIDARDB_MAJOR 0
#define VIDARDB_MINOR 1
#define VIDARDB_PATCH 0

// Do not use these. We made the mistake of declaring macros starting with
// double underscore. Now we have to live with our choice. We'll deprecate these
// at some point
#define __VIDARDB_MAJOR__ VIDARDB_MAJOR
#define __VIDARDB_MINOR__ VIDARDB_MINOR
#define __VIDARDB_PATCH__ VIDARDB_PATCH

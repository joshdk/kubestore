// Copyright Josh Komoroske. All rights reserved.
// Use of this source code is governed by the MIT license,
// a copy of which can be found in the LICENSE.txt file.

package kubestore

import "errors"

// ErrorKeyNotFound is a sentinel error for indicating that a key used when
// calling Store.Get was not found.
var ErrorKeyNotFound = errors.New("key not found")

// Copyright Josh Komoroske. All rights reserved.
// Use of this source code is governed by the MIT license,
// a copy of which can be found in the LICENSE.txt file.

package kubestore

import "context"

// Store represents a type that is capable of managing key/value pairs using
// some backing medium.
type Store interface {
	// Get retrieves the given key contents, and stores it into the given value
	// pointer. Returns ErrorKeyNotFound if the given key was not found.
	Get(ctx context.Context, key string, value interface{}) error

	// Set stores the given value under the given key.
	Set(ctx context.Context, key string, value interface{}) error

	// List returns a list of all keys.
	List(ctx context.Context) ([]string, error)

	// Delete removed the given key.
	Delete(ctx context.Context, key string) error
}

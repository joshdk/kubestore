// Copyright Josh Komoroske. All rights reserved.
// Use of this source code is governed by the MIT license,
// a copy of which can be found in the LICENSE.txt file.

package kubestore

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Assert that fileStore implements the Store interface.
var _ Store = fileStore{}

type fileStore struct {
	directory string
}

// NewFileStore returns a Store backed by files contained within the given
// directory.
//
// This Store is intended to be used as a fallback, or for testing outside of
// Kubernetes. It has no dependence on being run inside Kubernetes or on the
// Kubernetes API.
//
// This Store assumes full control of, and exclusive access to, the backing
// directory as it will be created on-demand when calling Store.Set and
// automatically deleted when calling Store.Delete (in the event that it does
// not contain any other files).
func NewFileStore(directory string) Store {
	return &fileStore{
		directory: directory,
	}
}

// Get reads the named file from the backing directory and stores the contents
// into the given value pointer.
//
// If the backing file does not exist, the ErrorKeyNotFound sentinel error
// is returned.
func (s fileStore) Get(_ context.Context, key string, value interface{}) error {
	// Determine the name of the backing file.
	filename := filepath.Join(s.directory, key)

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		// If the backing file does not exist, then return the not found
		// sentinel error.
		if os.IsNotExist(err) {
			return ErrorKeyNotFound
		}
		// Some other kind of error was encountered.
		return err
	}

	// Unmarshal the JSON data into the given value pointer.
	return json.Unmarshal(data, value)
}

// Set writes the given value into the backing file.
//
// If the backing directory does not exist, it is created on-demand.
func (s fileStore) Set(_ context.Context, key string, value interface{}) error {
	// Determine the name of the backing file.
	filename := filepath.Join(s.directory, key)

	// Marshal the the given value as JSON.
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	// Create a directory to contain the backing file.
	if err := os.MkdirAll(s.directory, 0755); err != nil {
		return err
	}

	// Write the value to the backing file.
	return ioutil.WriteFile(filename, data, 0644)
}

// List finds all files in the backing directory and returns a list of keys
// that can be used in subsequent calls to Store.Get or Store.Delete.
//
// If the backing directory does not exist, no keys are returned.
func (s fileStore) List(_ context.Context) ([]string, error) {
	// Stat all files in the backing directory.
	infos, err := ioutil.ReadDir(s.directory)
	if err != nil {
		// If the backing directory does not exist, then the keys also no not
		// exist, so return an empty (nil) slice.
		return nil, nil
	}

	// Build a list of all the keys.
	keys := make([]string, len(infos))
	for i, info := range infos {
		keys[i] = info.Name()
	}

	return keys, nil
}

// Delete removes the named file from the backing directory.
//
// If the backing directory is empty (if it contains no other files), it is
// also deleted.
func (s fileStore) Delete(_ context.Context, key string) error {
	// Determine the name of the backing file.
	filename := filepath.Join(s.directory, key)

	// Delete the backing file.
	if err := os.Remove(filename); err != nil {
		return err
	}

	// Delete the backing directory and intentionally ignore any errors, as
	// this is non-essential. os.Remove will return an error if the directory
	// contains other files, so we can safely call this without first checking
	// if said directory is empty.
	_ = os.Remove(s.directory)

	return nil
}

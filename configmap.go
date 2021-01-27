// Copyright Josh Komoroske. All rights reserved.
// Use of this source code is governed by the MIT license,
// a copy of which can be found in the LICENSE.txt file.

package kubestore

import (
	"context"
	"encoding/json"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

type configMapPatch struct {
	Data map[string]interface{} `json:"data,omitempty"`
}

// Assert that configMapStore implements the Store interface.
var _ Store = configMapStore{}

type configMapStore struct {
	client v1.ConfigMapInterface
	name   string
}

// NewConfigMapStore returns a Store backed by a ConfigMap with the given name.
//
// This Store is intended to be used when running inside of a pod, as it
// depends on the presence of a service account in order to interact with the
// Kubernetes API.
//
// This Store assumes full control of, and exclusive access to, the backing
// ConfigMap as it will be created on-demand when calling Store.Set and
// automatically deleted when calling Store.Delete (in the event that it is
// empty).
func NewConfigMapStore(name string) (Store, error) {
	// Lookup the current pod's service account details.
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	// Lookup the current pod's namespace.
	namespace, err := inClusterNamespace()
	if err != nil {
		return nil, err
	}

	// Create a set of Kubernetes clients.
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	// We're only interested in the ConfigMaps client.
	client := clientSet.CoreV1().ConfigMaps(namespace)

	return &configMapStore{
		client: client,
		name:   name,
	}, nil
}

// create is a helper for creating the backing ConfigMap.
func (c configMapStore) create(ctx context.Context) error {
	_, err := c.client.Create(ctx, &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: c.name,
		},
	}, metav1.CreateOptions{})
	return err
}

// delete is a helper for deleting the backing ConfigMap.
func (c configMapStore) delete(ctx context.Context) error {
	return c.client.Delete(ctx, c.name, metav1.DeleteOptions{})
}

// Get reads the named entry in the backing ConfigMap data and stores the
// contents into the given value pointer.
//
// If the backing ConfigMap does not exist, the ErrorKeyNotFound sentinel error
// is returned.
func (c configMapStore) Get(ctx context.Context, key string, value interface{}) error {
	// Use the Kuberneties API to get the backing ConfigMap.
	configMap, err := c.client.Get(ctx, c.name, metav1.GetOptions{})
	if err != nil {
		// If the backing ConfigMap does not exist, then the key also does not
		// exist, so return the not found sentinel error.
		if isResourceMissingError(err) {
			return ErrorKeyNotFound
		}
		// Some other kind of error was encountered.
		return err
	}

	// Lookup the given key in the ConfigMap's data.
	data, found := configMap.Data[key]
	if !found {
		// The given key does not exist in the ConfigMap data, so return the
		// not found sentinel error.
		return ErrorKeyNotFound
	}

	// Unmarshal the JSON data into the given value pointer.
	return json.Unmarshal([]byte(data), value)
}

// Set writes the named entry and value into the backing ConfigMap.
//
// If the backing ConfigMap does not exist, it is created on-demand.
func (c configMapStore) Set(ctx context.Context, key string, value interface{}) error {
	// Marshal the the given value as JSON.
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	// Construct a patch for setting the data value.
	patch := configMapPatch{
		Data: map[string]interface{}{
			key: string(data),
		},
	}

	// Convert the patch to JSON.
	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	// Use the Kuberneties API to patch the backing ConfigMap.
	_, err = c.client.Patch(ctx, c.name, types.MergePatchType, payload, metav1.PatchOptions{})
	if err != nil {
		if isResourceMissingError(err) {
			// If the backing ConfigMap does not exist, then create it
			// on-demand, and retry setting the value.
			if err := c.create(ctx); err != nil {
				return err
			}
			return c.Set(ctx, key, value)
		}
		// Some other kind of error was encountered.
		return err
	}

	return nil
}

// List finds all entries in the backing ConfigMap and returns a list of keys
// that can be used in subsequent calls to Store.Get or Store.Delete.
//
// If the backing ConfigMap does not exist, no keys are returned.
func (c configMapStore) List(ctx context.Context) ([]string, error) {
	// Use the Kuberneties API to get the backing ConfigMap.
	configMap, err := c.client.Get(ctx, c.name, metav1.GetOptions{})
	if err != nil {
		// If the backing ConfigMap does not exist, then the keys also no not
		// exist, so return an empty (nil) slice.
		if isResourceMissingError(err) {
			return nil, nil
		}
		// Some other kind of error was encountered.
		return nil, err
	}

	// Build a list of all the keys.
	keys := make([]string, 0, len(configMap.Data))
	for key := range configMap.Data {
		keys = append(keys, key)
	}

	return keys, nil
}

// Delete removes the named entry from the backing ConfigMap.
//
// If the backing ConfigMap is empty (if it has no data entries), it is then
// deleted.
func (c configMapStore) Delete(ctx context.Context, key string) error {
	// Construct a patch for deleting the data value.
	patch := configMapPatch{
		Data: map[string]interface{}{
			// Use a hardcoded value of null as that will cause the merge patch
			// to delete the named key.
			key: nil,
		},
	}

	// Convert the patch to JSON.
	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	// Use the Kuberneties API to patch the backing ConfigMap.
	configMap, err := c.client.Patch(ctx, c.name, types.MergePatchType, payload, metav1.PatchOptions{})
	if err != nil {
		// If the backing ConfigMap does not exist, then the key also does not
		// exist, so there's nothing else to do.
		if isResourceMissingError(err) {
			return nil
		}
		// Some other kind of error was encountered.
		return err
	}

	// Is the backing ConfigMap now empty?
	if len(configMap.Data) == 0 {
		// Delete the backing ConfigMap in order to clean up after ourselves.
		// Intentionally ignore any errors, as this is non-essential.
		_ = c.delete(ctx)
	}

	return nil
}

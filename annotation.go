// Copyright Josh Komoroske. All rights reserved.
// Use of this source code is governed by the MIT license,
// a copy of which can be found in the LICENSE.txt file.

package kubestore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const annotationPrefix = "kubestore"

type annotationPatch struct {
	Metadata metadataPatch `json:"metadata,omitempty"`
}

type metadataPatch struct {
	Annotations map[string]interface{} `json:"annotations,omitempty"`
}

// Assert that annotationStore implements the Store interface.
var _ Store = annotationStore{}

type annotationStore struct {
	client dynamic.ResourceInterface
	name   string
}

// NewAnnotationStore returns a Store backed by the annotations on a resource.
//
// This Store is intended to be used when running inside of a pod, as it
// depends on the presence of a service account in order to interact with the
// Kubernetes API.
func NewAnnotationStore(group, version, resource, name string) (Store, error) {
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

	// Create a dynamic Kubernetes client.
	dynclient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	// We're only interested in the client for this specific resource.
	gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
	client := dynclient.Resource(gvr).Namespace(namespace)

	return &annotationStore{
		client: client,
		name:   name,
	}, nil
}

// Get reads the named annotation from the backing resource and stores the
// contents into the given value pointer.
//
// If the backing resource does not exist, the ErrorKeyNotFound sentinel error
// is returned.
func (c annotationStore) Get(ctx context.Context, key string, value interface{}) error {
	// Construct the full annotation.
	annotation := fmt.Sprintf("%s/%s", annotationPrefix, key)

	// Use the Kuberneties API to get the backing resource.
	resource, err := c.client.Get(ctx, c.name, metav1.GetOptions{})
	if err != nil {
		// If the backing resource does not exist, then the key also does not
		// exist, so return the not found sentinel error.
		if isResourceMissingError(err) {
			return ErrorKeyNotFound
		}
		// Some other kind of error was encountered.
		return err
	}

	// Lookup the desired resource annotation.
	data, found := resource.GetAnnotations()[annotation]
	if !found {
		// The desired annotation does not exist, so return the not found
		// sentinel error.
		return ErrorKeyNotFound
	}

	// Unmarshal the JSON data into the given value pointer.
	return json.Unmarshal([]byte(data), value)
}

// Set writes the named entry and value into the backing resource annotations.
func (c annotationStore) Set(ctx context.Context, key string, value interface{}) error {
	// Construct the full annotation.
	annotation := fmt.Sprintf("%s/%s", annotationPrefix, key)

	// Marshal the the given value as JSON.
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	// Construct a patch for setting the annotation value.
	patch := annotationPatch{
		Metadata: metadataPatch{
			Annotations: map[string]interface{}{
				annotation: string(data),
			},
		},
	}

	// Convert the patch to JSON.
	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	// Use the Kuberneties API to patch the backing resource.
	_, err = c.client.Patch(ctx, c.name, types.MergePatchType, payload, metav1.PatchOptions{})
	return err
}

// List finds all matching annotations in the backing resource and returns a
// list of keys that can be used in subsequent calls to Store.Get or
// Store.Delete.
//
// If the backing resource does not exist, no keys are returned.
func (c annotationStore) List(ctx context.Context) ([]string, error) {
	// Use the Kuberneties API to get the backing resource.
	resource, err := c.client.Get(ctx, c.name, metav1.GetOptions{})
	if err != nil {
		// If the backing resource does not exist, then the keys also no not
		// exist, so return an empty (nil) slice.
		if isResourceMissingError(err) {
			return nil, nil
		}
		// Some other kind of error was encountered.
		return nil, err
	}

	// Build a list of all the keys.
	var keys []string
	for annotation := range resource.GetAnnotations() {
		// Disregard annotation that do not match.
		if !strings.HasPrefix(annotation, annotationPrefix+"/") {
			continue
		}
		key := strings.TrimPrefix(annotation, annotationPrefix+"/")
		keys = append(keys, key)
	}

	return keys, nil
}

// Delete removes the named annotation from the backing resource.
func (c annotationStore) Delete(ctx context.Context, key string) error {
	// Construct the full annotation.
	annotation := fmt.Sprintf("%s/%s", annotationPrefix, key)

	// Construct a patch for deleting the annotation.
	patch := annotationPatch{
		Metadata: metadataPatch{
			Annotations: map[string]interface{}{
				annotation: nil,
			},
		},
	}

	// Convert the patch to JSON.
	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	// Use the Kuberneties API to patch the backing resource.
	_, err = c.client.Patch(ctx, c.name, types.MergePatchType, payload, metav1.PatchOptions{})
	if err != nil {
		// If the backing resource does not exist, then the key also does not
		// exist, so there's nothing else to do.
		if isResourceMissingError(err) {
			return nil
		}
		// Some other kind of error was encountered.
		return err
	}

	return nil
}

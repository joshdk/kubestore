// Copyright Josh Komoroske. All rights reserved.
// Use of this source code is governed by the MIT license,
// a copy of which can be found in the LICENSE.txt file.

package kubestore

import (
	"io/ioutil"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
)

// inClusterNamespace reads the namespace for the current pod.
func inClusterNamespace() (string, error) {
	// Read the namespace associated with the service account token, if available.
	data, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// isResourceMissingError returns true if the given error indicates that a
// Kubernetes API call failed because the targeted resource did not exist.
func isResourceMissingError(err error) bool {
	if sterr, ok := err.(*errors.StatusError); ok {
		return sterr.ErrStatus.Code == 404
	}
	return false
}

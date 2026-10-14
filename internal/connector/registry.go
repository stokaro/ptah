package connector

import (
	"errors"
	"fmt"
)

// ErrConflict identifies a registration that names a dialect another
// registration of the same registry already serves.
var ErrConflict = errors.New("conflicting connector registration")

// Registration names the dialect a service connects to.
type Registration struct {
	// Dialect is the dialect's canonical name, as its descriptor spells it.
	Dialect string
	// Service connects to it.
	Service Service
}

// Registry is an immutable set of registrations, one service per dialect. A
// nil *Registry is empty: every lookup misses.
type Registry struct {
	services map[string]Service
}

// NewRegistry returns the registrations as a registry. Two registrations of
// one dialect fail with [ErrConflict], and a registration without a dialect or
// without a service is refused.
func NewRegistry(registrations ...Registration) (*Registry, error) {
	services := make(map[string]Service, len(registrations))
	for _, registration := range registrations {
		if registration.Dialect == "" || registration.Service == nil {
			return nil, fmt.Errorf("connector registration %+v names no dialect or no service", registration)
		}
		if _, taken := services[registration.Dialect]; taken {
			return nil, fmt.Errorf("%w: dialect %q is registered twice", ErrConflict, registration.Dialect)
		}
		services[registration.Dialect] = registration.Service
	}
	return &Registry{services: services}, nil
}

// Lookup returns the service registered for the dialect a canonical name
// names.
func (r *Registry) Lookup(dialect string) (Service, bool) {
	if r == nil {
		return nil, false
	}
	service, ok := r.services[dialect]
	return service, ok
}

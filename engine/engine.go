// Package engine assembles explicit providers for the schema pipeline. It
// imports rendering contracts, but no built-in database implementations.
package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// ErrInvalidRegistration identifies a malformed provider descriptor or
// conflicting ownership. A failed registration produces no runtime.
var ErrInvalidRegistration = errors.New("invalid provider registration")

// Provider describes services supplied by one explicitly selected provider.
// ID is its stable, namespaced owner identity, separate from target names and
// payload format versions. New copies the descriptor; services themselves
// must remain safe for concurrent calls throughout the runtime's lifetime.
type Provider struct {
	// ID identifies the provider independently of its implementation version.
	ID string
	// Targets lists the target names and services owned by this provider.
	Targets []Target
}

// Target declares a canonical target name, accepted aliases, and its optional
// rendering service. Names use lowercase ASCII letters, digits, '-' and '_',
// beginning with a letter. Registering a target does not assert that its server
// supports any particular capability. A nil Rendering service is unavailable.
type Target struct {
	// Name is the canonical target identifier passed to the service.
	Name string
	// Aliases contains additional accepted spellings, excluding Name.
	Aliases []string
	// Rendering is optional. A typed-nil service is an invalid registration.
	Rendering renderer.Service
}

// Runtime is a frozen selection of providers. It is safe for concurrent calls
// when its services satisfy their contracts. The zero value has no providers;
// it refuses every target and never falls back to a built-in implementation.
type Runtime struct {
	targets map[string]target
}

type target struct {
	owner     string
	name      string
	rendering renderer.Service
}

// New validates and freezes explicit provider ownership. Duplicate provider
// IDs, target names, and aliases are errors, including duplicates within one
// provider. Registration calls no services and performs no database I/O.
func New(providers ...Provider) (*Runtime, error) {
	runtime := &Runtime{targets: make(map[string]target)}
	owners := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		if !validOwner(provider.ID) {
			return nil, fmt.Errorf("%w: invalid provider identity %q", ErrInvalidRegistration, provider.ID)
		}
		if _, found := owners[provider.ID]; found {
			return nil, fmt.Errorf("%w: duplicate provider %q", ErrInvalidRegistration, provider.ID)
		}
		owners[provider.ID] = struct{}{}
		for _, declared := range provider.Targets {
			if err := runtime.register(provider.ID, declared); err != nil {
				return nil, err
			}
		}
	}
	return runtime, nil
}

func (r *Runtime) register(owner string, declared Target) error {
	if !validName(declared.Name) {
		return fmt.Errorf("%w: invalid target %q", ErrInvalidRegistration, declared.Name)
	}
	if declared.Rendering != nil && nilService(declared.Rendering) {
		return fmt.Errorf("%w: target %q has a typed-nil rendering service", ErrInvalidRegistration, declared.Name)
	}
	for _, name := range append([]string{declared.Name}, declared.Aliases...) {
		if !validName(name) {
			return fmt.Errorf("%w: invalid target alias %q", ErrInvalidRegistration, name)
		}
		if existing, found := r.targets[name]; found {
			return fmt.Errorf("%w: target name %q is claimed by %q and %q",
				ErrInvalidRegistration, name, existing.owner, owner)
		}
		r.targets[name] = target{owner: owner, name: declared.Name, rendering: declared.Rendering}
	}
	return nil
}

// Targets returns canonical target names, sorted and without aliases. The
// result is a fresh slice. A nil or zero runtime returns an empty slice.
func (r *Runtime) Targets() []string {
	result := make([]string, 0)
	if r == nil {
		return result
	}
	for name, target := range r.targets {
		if name == target.name {
			result = append(result, name)
		}
	}
	slices.Sort(result)
	return result
}

// Render dispatches a complete batch to its single selected owner. Target
// names are matched without surrounding whitespace and ASCII case. Unknown
// targets satisfy errors.Is(err, ptaherr.ErrUnsupportedDialect); unavailable
// rendering satisfies errors.Is(err, ptaherr.ErrUnsupportedFeature).
//
// Cancellation and service errors propagate without partial SQL. A nil context
// is rejected. The service receives a cloned capability set and node slice;
// its contract also prohibits mutation of the nodes themselves.
func (r *Runtime) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	if ctx == nil {
		return renderer.Result{}, errors.New("rendering requires a context")
	}
	if err := ctx.Err(); err != nil {
		return renderer.Result{}, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return renderer.Result{}, &ptaherr.RenderError{
			Dialect: request.Target,
			Err:     ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("unsupported database dialect: %s", request.Target),
		}
	}
	if selected.rendering == nil {
		return renderer.Result{}, &ptaherr.CapabilityError{
			Dialect: selected.name,
			Feature: "rendering",
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("provider %q does not offer rendering for %q", selected.owner, selected.name),
		}
	}
	request.Target = selected.name
	request.Capabilities = request.Capabilities.Clone()
	request.Nodes = slices.Clone(request.Nodes)
	result, err := selected.rendering.Render(ctx, request)
	if err != nil {
		return renderer.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return renderer.Result{}, err
	}
	return result, nil
}

func (r *Runtime) lookup(name string) (target, bool) {
	if r == nil {
		return target{}, false
	}
	selected, found := r.targets[strings.ToLower(strings.TrimSpace(name))]
	return selected, found
}

func validName(name string) bool {
	if name == "" || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '_' {
			return false
		}
	}
	return true
}

func validOwner(owner string) bool {
	parts := strings.Split(owner, "/")
	if len(parts) < 2 || !strings.Contains(parts[0], ".") {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, " \t\r\n\\") {
			return false
		}
	}
	return true
}

func nilService(service renderer.Service) bool {
	value := reflect.ValueOf(service)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

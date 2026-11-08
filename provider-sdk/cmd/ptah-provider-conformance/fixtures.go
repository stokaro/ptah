package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"ptah.run/provider-sdk/conformance"
	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// fixtureFile is the JSON form of what the suite works with. A request, a
// unit and a render request are the protocol's messages in their JSON
// mapping, so a provider in any language writes them.
type fixtureFile struct {
	Calls []struct {
		Name    string          `json:"name"`
		Method  string          `json:"method"`
		Request json.RawMessage `json:"request"`
		Refused bool            `json:"refused"`
	} `json:"calls"`
	Models []conformance.ModelIdentity `json:"models"`
	Live   struct {
		Schemas        []string `json:"schemas"`
		LockName       string   `json:"lock_name"`
		InProcessLocks bool     `json:"in_process_locks"`
		Changes        []struct {
			Name   string      `json:"name"`
			Apply  fixtureStep `json:"apply"`
			Revert fixtureStep `json:"revert"`
		} `json:"changes"`
		Refused []json.RawMessage `json:"refused"`
	} `json:"live"`
}

type fixtureStep struct {
	Render json.RawMessage   `json:"render"`
	Units  []json.RawMessage `json:"units"`
}

// fixtures is a decoded fixture file.
type fixtures struct {
	calls    []conformance.Call
	models   []conformance.ModelIdentity
	schemas  []string
	lockName string
	// inProcessLocks is live.in_process_locks: see conformance.Live.InProcessLocks.
	inProcessLocks bool
	changes        []conformance.Change
	refused        []*protocol.Unit
}

func readFixtures(path string) (*fixtures, error) {
	content, err := os.ReadFile(path) // #nosec G304 -- the operator names the fixture file
	if err != nil {
		return nil, fmt.Errorf("read the fixtures: %w", err)
	}
	var file fixtureFile
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, fmt.Errorf("decode the fixtures: %w", err)
	}
	loaded := &fixtures{models: file.Models, schemas: file.Live.Schemas, lockName: file.Live.LockName, inProcessLocks: file.Live.InProcessLocks}
	for _, call := range file.Calls {
		request, err := methodRequest(call.Method, call.Request)
		if err != nil {
			return nil, fmt.Errorf("call %s: %w", call.Name, err)
		}
		loaded.calls = append(loaded.calls, conformance.Call{Name: call.Name, Request: request, Refused: call.Refused})
	}
	for _, change := range file.Live.Changes {
		apply, err := decodeStep(change.Apply)
		if err != nil {
			return nil, fmt.Errorf("change %s: %w", change.Name, err)
		}
		revert, err := decodeStep(change.Revert)
		if err != nil {
			return nil, fmt.Errorf("change %s: %w", change.Name, err)
		}
		loaded.changes = append(loaded.changes, conformance.Change{Name: change.Name, Apply: apply, Revert: revert})
	}
	if loaded.refused, err = decodeUnits(file.Live.Refused); err != nil {
		return nil, fmt.Errorf("refused units: %w", err)
	}
	return loaded, nil
}

// methodRequest decodes the request of a method of the provider service.
func methodRequest(method string, raw json.RawMessage) (proto.Message, error) {
	service := protocol.File_protocol_experimental_provider_proto.Services().ByName("Provider")
	descriptor := service.Methods().ByName(protoreflect.Name(method))
	if descriptor == nil {
		return nil, fmt.Errorf("the provider service has no method %q", method)
	}
	found, err := protoregistry.GlobalTypes.FindMessageByName(descriptor.Input().FullName())
	if err != nil {
		return nil, err
	}
	request := found.New().Interface()
	if err := protojson.Unmarshal(raw, request); err != nil {
		return nil, fmt.Errorf("decode the %s request: %w", method, err)
	}
	return request, nil
}

func decodeStep(step fixtureStep) (conformance.Step, error) {
	var decoded conformance.Step
	if len(step.Render) != 0 {
		decoded.Render = &protocol.RenderRequest{}
		if err := protojson.Unmarshal(step.Render, decoded.Render); err != nil {
			return decoded, fmt.Errorf("decode the render request: %w", err)
		}
	}
	units, err := decodeUnits(step.Units)
	decoded.Units = units
	return decoded, err
}

func decodeUnits(raw []json.RawMessage) ([]*protocol.Unit, error) {
	units := make([]*protocol.Unit, 0, len(raw))
	for _, entry := range raw {
		unit := &protocol.Unit{}
		if err := protojson.Unmarshal(entry, unit); err != nil {
			return nil, fmt.Errorf("decode a unit: %w", err)
		}
		units = append(units, unit)
	}
	return units, nil
}

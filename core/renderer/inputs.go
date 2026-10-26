package renderer

import "fmt"

// InputEnvironment identifies a value supplied by the host process environment
// immediately before execution. Plans record its name and never its value.
const InputEnvironment = "environment"

// InputReference names an execution input without reading or storing its value.
// A caller approves these references together with the exact operation body.
type InputReference struct {
	Source string `json:"source"`
	Name   string `json:"name"`
}

// ValidateInputReferences requires supported sources and sorted unique names.
// Environment names use the portable ASCII identifier grammar.
func ValidateInputReferences(inputs []InputReference) error {
	previous := ""
	for _, input := range inputs {
		if input.Source != InputEnvironment || !environmentInputName(input.Name) {
			return fmt.Errorf("%w: execution input needs an environment source and a valid name", ErrInvalidResult)
		}
		if input.Name <= previous {
			return fmt.Errorf("%w: execution inputs must have unique sorted names", ErrInvalidResult)
		}
		previous = input.Name
	}
	return nil
}

func environmentInputName(name string) bool {
	if name == "" {
		return false
	}
	for i, char := range name {
		if char != '_' && (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') && (i == 0 || char < '0' || char > '9') {
			return false
		}
	}
	return true
}

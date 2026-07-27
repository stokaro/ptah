package dbtest

import (
	"fmt"
	"regexp"
	"slices"
)

// FilterCases returns cases whose names match pattern. Pattern uses Go regular
// expression syntax. An empty pattern selects every case. The returned outer
// slice does not alias the input; nested Step and Assertion data is shared.
func FilterCases(cases []Case, pattern string) ([]Case, error) {
	if pattern == "" {
		return slices.Clone(cases), nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("compile test case pattern %q: %w", pattern, err)
	}

	filtered := make([]Case, 0, len(cases))
	for _, testCase := range cases {
		if re.MatchString(testCase.Name) {
			filtered = append(filtered, testCase)
		}
	}
	return filtered, nil
}

package embedprovider_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedprovider"
)

// vectors builds a result of the given shape.
func vectors(values ...[]float32) embedprovider.Result {
	result := embedprovider.Result{}
	for _, value := range values {
		result.Vectors = append(result.Vectors, value)
	}
	return result
}

// TestValidateResult_RefusesWhatWouldPoisonTheCorpus is the whole reason
// validation is not optional.
//
// Every row here is an answer a provider can give that a caller cannot detect
// afterwards: once a vector is stored it is a list of numbers, and the corpus
// retrieves whatever it holds. A wrong vector is not a failed run, it is a
// working system returning the wrong documents (stokaro/ptah#2068).
func TestValidateResult_RefusesWhatWouldPoisonTheCorpus(t *testing.T) {
	tests := []struct {
		name      string
		result    embedprovider.Result
		inputs    int
		dimension int
		wantMatch string
	}{
		{
			name:   "a partial batch is not a complete one",
			result: vectors([]float32{1, 2}), inputs: 2, dimension: 2,
			wantMatch: `.*2 inputs and 1 vectors.*partial batch.*`,
		},
		{
			name:   "more vectors than inputs",
			result: vectors([]float32{1, 2}, []float32{3, 4}), inputs: 1, dimension: 2,
			wantMatch: `.*1 inputs and 2 vectors.*`,
		},
		{
			name:   "the wrong dimension",
			result: vectors([]float32{1, 2, 3}), inputs: 1, dimension: 2,
			wantMatch: `.*vector 0 has 3 dimensions and the generation expects 2.*`,
		},
		{
			name:   "an empty vector",
			result: vectors(make([]float32, 0)), inputs: 1, dimension: 0,
			wantMatch: `.*vector 0 is empty.*`,
		},
		{
			name:   "NaN",
			result: vectors([]float32{1, float32(math.NaN())}), inputs: 1, dimension: 2,
			wantMatch: `.*vector 0 component 1 is NaN.*`,
		},
		{
			name:   "positive infinity",
			result: vectors([]float32{float32(math.Inf(1)), 2}), inputs: 1, dimension: 2,
			wantMatch: `.*vector 0 component 0 is infinite.*`,
		},
		{
			name:   "negative infinity",
			result: vectors([]float32{1, float32(math.Inf(-1))}), inputs: 1, dimension: 2,
			wantMatch: `.*vector 0 component 1 is infinite.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := embedprovider.ValidateResult(test.result, test.inputs, test.dimension)

			c.Assert(err, qt.ErrorIs, embedprovider.ErrInvalidResponse)
			c.Assert(err, qt.ErrorMatches, test.wantMatch)
		})
	}
}

// TestValidateResult_AcceptsAnAnswerThatIsRight is the control.
//
// A validator that refused everything would satisfy every row above and stop
// the product working, which is the failure mode a one-sided suite cannot see.
func TestValidateResult_AcceptsAnAnswerThatIsRight(t *testing.T) {
	tests := []struct {
		name      string
		result    embedprovider.Result
		inputs    int
		dimension int
	}{
		{name: "one vector", result: vectors([]float32{1, 2}), inputs: 1, dimension: 2},
		{
			name:   "a full batch",
			result: vectors([]float32{1, 2}, []float32{3, 4}), inputs: 2, dimension: 2,
		},
		{
			name:   "no dimension expected yet",
			result: vectors([]float32{1, 2, 3}), inputs: 1, dimension: 0,
		},
		{
			name:   "zero is a value, not a missing one",
			result: vectors([]float32{0, 0}), inputs: 1, dimension: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(embedprovider.ValidateResult(test.result, test.inputs, test.dimension), qt.IsNil)
		})
	}
}

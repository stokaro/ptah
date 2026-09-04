package embedrun

import "fmt"

// BatchBounds are the limits a batch has to respect at once.
//
// They come from four different places and none of them is negotiable: the
// provider's own limits, the memory of the process assembling the request, and
// the operator's wish to be able to stop. A batch sized by one of them alone
// fails against the others, so the size is the smallest answer they all permit
// (stokaro/ptah#2068).
type BatchBounds struct {
	// MaxInputs is the provider's limit on inputs per request. Zero means the
	// provider does not batch, which is one input per request.
	MaxInputs int
	// MaxBytes is the provider's limit on the total bytes of one request. Zero
	// means unbounded.
	MaxBytes int
	// MaxLocalBytes bounds what this process assembles in memory at once. Zero
	// means unbounded.
	MaxLocalBytes int
	// MaxRows bounds the rows read from the source in one query, which is what
	// keeps a scan responsive to cancellation: a batch that takes a minute
	// cannot be stopped in less than a minute.
	MaxRows int
}

// Batch is one unit of work: the inputs, and the rows they came from.
type Batch struct {
	// Rows are the source rows this batch covers, in scan order.
	Rows []BatchRow
	// Bytes is the total size of the inputs.
	Bytes int
}

// BatchRow is one row and the text that will be embedded for it.
type BatchRow struct {
	// Key is the source key, in the specification's key order.
	Key []string
	// Input is the canonical text.
	Input string
	// Version is the source version read with the row.
	Version string
	// InputHash is the source-input hash of Input under this generation.
	InputHash string
	// Skipped marks a row the specification declined to embed. It travels in
	// the batch rather than being dropped, because a skipped row is a fact
	// verification needs -- silent omission is what the epic forbids.
	Skipped bool
	// SkipReason says why.
	SkipReason string
	// Ordinal is this row's position in its source key's chunk set, and is
	// zero for a specification that does not chunk.
	//
	// A batch therefore carries several rows with one key, and that is the
	// point: a batch is a unit of provider work, and each chunk is one call's
	// worth of it.
	Ordinal int
}

// Assemble packs rows into batches that respect every bound at once.
//
// Adaptation is allowed and identity is not affected by it: the epic says batch
// size may adapt, and nothing about a batch enters the generation identity, so
// two runs that batched differently produce the same vectors.
//
// A skipped row occupies a place in a batch and contributes no bytes: it is not
// sent to the provider, and dropping it here would lose the record verification
// reads as a deliberate gap.
func Assemble(rows []BatchRow, bounds BatchBounds) ([]Batch, error) {
	if err := bounds.validate(rows); err != nil {
		return nil, err
	}

	var batches []Batch
	current := Batch{}
	for _, row := range rows {
		size := len(row.Input)
		if row.Skipped {
			size = 0
		}
		if len(current.Rows) > 0 && !bounds.fits(current, size) {
			batches = append(batches, current)
			current = Batch{}
		}
		current.Rows = append(current.Rows, row)
		current.Bytes += size
	}
	if len(current.Rows) > 0 {
		batches = append(batches, current)
	}
	return batches, nil
}

// fits reports whether one more row of the given size stays inside every bound.
func (b BatchBounds) fits(batch Batch, size int) bool {
	inputs := b.MaxInputs
	if inputs <= 0 {
		inputs = 1
	}
	rows := b.MaxRows
	if rows <= 0 {
		rows = inputs
	}
	switch {
	case len(batch.Rows)+1 > inputs, len(batch.Rows)+1 > rows:
		return false
	case b.MaxBytes > 0 && batch.Bytes+size > b.MaxBytes:
		return false
	case b.MaxLocalBytes > 0 && batch.Bytes+size > b.MaxLocalBytes:
		return false
	default:
		return true
	}
}

// validate refuses a row no batch could ever hold.
//
// A single input over the byte bound would otherwise sit alone in a batch that
// still exceeds it, and the provider would refuse the request -- after the scan,
// the canonicalization and the hash. The specification's own truncation policy
// is what should have caught it, and saying so names the fix.
func (b BatchBounds) validate(rows []BatchRow) error {
	bound := b.MaxBytes
	if b.MaxLocalBytes > 0 && (bound == 0 || b.MaxLocalBytes < bound) {
		bound = b.MaxLocalBytes
	}
	if bound <= 0 {
		return nil
	}
	for _, row := range rows {
		if !row.Skipped && len(row.Input) > bound {
			return fmt.Errorf(
				"batch assembly: the input for key %v is %d bytes and no batch may exceed %d; "+
					"set the specification's max input bytes and truncation policy",
				row.Key, len(row.Input), bound)
		}
	}
	return nil
}

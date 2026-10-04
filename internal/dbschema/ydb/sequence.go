package ydb

import (
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"ptah.run/catalog"
	"ptah.run/internal/ydbsequence"
	"ptah.run/internal/ydbtype"
)

// sequenceDataTypeField is the number of the sequence description's
// `data_type`, a field the pinned protocol buffers do not model. Measured on
// 25.1.4.7 and 26.2.1.14, every Serial's sequence reports it, and as Int64
// whatever the column's width.
const sequenceDataTypeField protowire.Number = 9

// sequence reads the description of the sequence behind Serial column column,
// whose YDB type is ydbType, onto column: its start and its increment, each
// only where it is not the default 1, so a Serial nobody altered reads the way
// a declaration that names neither writes, and the value of the last RESTART,
// which YDB replays on every later ALTER SEQUENCE.
//
// Only what ALTER SEQUENCE can set is read, and the rest has to be what a new
// sequence has, because a description that differed would be a sequence the
// declaration cannot state and the plan cannot reproduce: a name other than
// the one the planner addresses, a least value other than 1, a cache other
// than 1, a sequence that cycles, or a field the pinned protocol buffers do not
// model. Each is refused by name. A maximum is either the column type's or the
// Int64 maximum: every ALTER SEQUENCE moves a Serial's or a SmallSerial's
// maximum to the latter, so both are sequences YDB makes.
func sequence(column *catalog.Column, ydbType string, described *Ydb_Table.SequenceDescription) error {
	if err := sequenceExtraFields(described); err != nil {
		return err
	}
	if described.Name != nil && described.GetName() != ydbsequence.Name(column.Name) {
		return fmt.Errorf("its sequence is named %q rather than %q, the name Ptah addresses it by",
			described.GetName(), ydbsequence.Name(column.Name))
	}
	switch {
	case described.MinValue != nil && described.GetMinValue() != 1:
		return fmt.Errorf("its sequence has the least value %d, and Ptah reads a sequence whose least value is 1",
			described.GetMinValue())
	case described.Cache != nil && described.GetCache() != 1:
		return fmt.Errorf("its sequence caches %d values, and Ptah reads a sequence that caches 1", described.GetCache())
	case described.GetCycle():
		return fmt.Errorf("its sequence cycles, and Ptah reads a sequence that does not")
	}
	if described.MaxValue != nil && !slices.Contains(sequenceMaxima(ydbType), described.GetMaxValue()) {
		return fmt.Errorf("its sequence has the maximum %d, which is neither the maximum of %s nor the Int64 "+
			"maximum an ALTER SEQUENCE leaves", described.GetMaxValue(), ydbType)
	}
	if start := described.GetStartValue(); described.StartValue != nil && start != 1 {
		column.IdentityStart = strconv.FormatInt(start, 10)
	}
	if increment := described.GetIncrement(); described.Increment != nil && increment != 1 {
		column.IdentityIncrement = strconv.FormatInt(increment, 10)
	}
	if restart := described.GetSetVal(); restart != nil {
		// Present only on a sequence some ALTER SEQUENCE restarted, holding
		// the value of the restart rather than the sequence's next value.
		column.SequenceRestart = strconv.FormatInt(restart.GetNextValue(), 10)
	}
	return nil
}

// sequenceMaxima are the maxima a Serial's sequence may report for a column of
// ydbType: the column type's, and the Int64 maximum an ALTER SEQUENCE leaves.
func sequenceMaxima(ydbType string) []int64 {
	switch ydbType {
	case ydbtype.Int16:
		return []int64{math.MaxInt16, math.MaxInt64}
	case ydbtype.Int32:
		return []int64{math.MaxInt32, math.MaxInt64}
	default:
		return []int64{math.MaxInt64}
	}
}

// sequenceExtraFields refuses a field of the sequence description the pinned
// protocol buffers do not model, except the data type, which it reads itself
// and accepts as Int64 only.
func sequenceExtraFields(described *Ydb_Table.SequenceDescription) error {
	unknown := described.ProtoReflect().GetUnknown()
	for len(unknown) > 0 {
		number, wireType, length := protowire.ConsumeTag(unknown)
		if length < 0 {
			return fmt.Errorf("its sequence description does not decode: %w", protowire.ParseError(length))
		}
		unknown = unknown[length:]
		valueLength := protowire.ConsumeFieldValue(number, wireType, unknown)
		if valueLength < 0 {
			return fmt.Errorf("its sequence description does not decode: %w", protowire.ParseError(valueLength))
		}
		value := unknown[:valueLength]
		unknown = unknown[valueLength:]
		if number != sequenceDataTypeField || wireType != protowire.BytesType {
			return fmt.Errorf("its sequence carries field %d of its description, which this build of Ptah does not read",
				number)
		}
		payload, _ := protowire.ConsumeBytes(value)
		var dataType Ydb.Type
		if err := proto.Unmarshal(payload, &dataType); err != nil {
			return fmt.Errorf("its sequence's data type does not decode: %w", err)
		}
		if dataType.GetTypeId() != Ydb.Type_INT64 {
			return fmt.Errorf("its sequence's data type is %s, and Ptah reads a sequence of Int64", dataType.String())
		}
	}
	return nil
}

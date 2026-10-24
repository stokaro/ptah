package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// validateEmbeddingColumns refuses an empty list, an empty or repeated name,
// and a name that is not text.
func validateEmbeddingColumns(names []string) error {
	if len(names) == 0 {
		return fmt.Errorf("%w: an embedding source names no column", ErrProtocol)
	}
	for i, name := range names {
		if !embeddingName(name) || slices.Contains(names[:i], name) {
			return fmt.Errorf("%w: an empty, invalid, or repeated embedding column", ErrProtocol)
		}
	}
	return nil
}

// validateEmbeddingSource checks the relation, its key and input fields, the
// version field and the filter. The filter is the specification's own text
// in the engine's dialect, and is checked only as text.
func validateEmbeddingSource(source *protocol.EmbeddingSource) error {
	if err := validateEmbeddingTable(source.GetTable()); err != nil {
		return err
	}
	if err := validateEmbeddingColumns(source.GetKeyFields()); err != nil {
		return err
	}
	if err := validateEmbeddingColumns(source.GetInputFields()); err != nil {
		return err
	}
	if !embeddingTexts(source.GetVersionField(), source.GetFilter()) {
		return fmt.Errorf("%w: an embedding version field or filter is not text", ErrProtocol)
	}
	return nil
}

// validateEmbeddingTarget checks the relation, the column, a known layout and
// metric, the representation, a dimension within the bound, and the index
// method and options.
func validateEmbeddingTarget(target *protocol.EmbeddingTarget) error {
	if err := validateEmbeddingTable(target.GetTable()); err != nil {
		return err
	}
	if !embeddingName(target.GetColumn()) || !embeddingName(target.GetRepresentation()) {
		return fmt.Errorf("%w: an embedding target needs a column and a representation", ErrProtocol)
	}
	layout, metric := target.GetLayout(), target.GetMetric()
	if _, known := protocol.EmbeddingTarget_Layout_name[int32(layout)]; !known || layout == protocol.EmbeddingTarget_LAYOUT_UNSPECIFIED {
		return fmt.Errorf("%w: an embedding target needs a known layout", ErrProtocol)
	}
	if _, known := protocol.EmbeddingTarget_Metric_name[int32(metric)]; !known || metric == protocol.EmbeddingTarget_METRIC_UNSPECIFIED {
		return fmt.Errorf("%w: an embedding target needs a known metric", ErrProtocol)
	}
	if target.GetDimension() == 0 || target.GetDimension() > MaxEmbeddingDimension {
		return fmt.Errorf("%w: an embedding dimension is between 1 and %d", ErrProtocol, MaxEmbeddingDimension)
	}
	if !embeddingText(target.GetIndexMethod()) {
		return fmt.Errorf("%w: an embedding index method is not text", ErrProtocol)
	}
	for name, value := range target.GetIndexOptions() {
		if !embeddingName(name) || !embeddingText(value) {
			return fmt.Errorf("%w: an embedding index option is not text", ErrProtocol)
		}
	}
	return nil
}

// validateEmbeddingNames requires names that are text.
func validateEmbeddingNames(names *protocol.EmbeddingNames) error {
	if !embeddingTexts(names.GetIndex(), names.GetOwnershipMark(), names.GetColumnComment()) {
		return fmt.Errorf("%w: an embedding object name is not text", ErrProtocol)
	}
	return nil
}

// validateEmbeddingSpec checks what an engine needs of a specification. A
// chunked specification keeps a set of rows per source key, which needs a
// table of its own.
func validateEmbeddingSpec(spec *protocol.EmbeddingSpec) error {
	if !embeddingName(spec.GetGeneration()) {
		return fmt.Errorf("%w: an embedding request names no generation", ErrProtocol)
	}
	if err := validateEmbeddingSource(spec.GetSource()); err != nil {
		return err
	}
	if err := validateEmbeddingTarget(spec.GetTarget()); err != nil {
		return err
	}
	owned := spec.GetTarget().GetLayout() == protocol.EmbeddingTarget_OWN_TABLE
	if spec.GetChunked() && !owned {
		return fmt.Errorf("%w: a chunked generation keeps its sets in a table of its own", ErrProtocol)
	}
	names := spec.GetNames()
	if spec.GetTarget().GetIndexMethod() != "" && names.GetIndex() == "" || owned && names.GetOwnershipMark() == "" {
		return fmt.Errorf("%w: an embedding request lacks a name its objects need", ErrProtocol)
	}
	return validateEmbeddingNames(names)
}

// validateEmbeddingLocation checks a registered generation's location. The
// index name is always required: a retirement drops the index whether or not
// the specification declared one.
func validateEmbeddingLocation(location *protocol.EmbeddingLocation) error {
	if !embeddingName(location.GetGeneration()) || !embeddingName(location.GetColumn()) {
		return fmt.Errorf("%w: an embedding location needs a generation and a column", ErrProtocol)
	}
	if err := validateEmbeddingTable(location.GetTable()); err != nil {
		return err
	}
	if location.GetDimension() > MaxEmbeddingDimension {
		return fmt.Errorf("%w: an embedding dimension is at most %d", ErrProtocol, MaxEmbeddingDimension)
	}
	if location.GetNames().GetIndex() == "" {
		return fmt.Errorf("%w: an embedding location names no index", ErrProtocol)
	}
	return validateEmbeddingNames(location.GetNames())
}

// ValidateDescribeEmbeddingTargetRequest checks the negotiated service, the
// engine and the specification.
func ValidateDescribeEmbeddingTargetRequest(description *protocol.Description, request *protocol.DescribeEmbeddingTargetRequest) error {
	if err := embeddingEngine(description, request.GetEngine()); err != nil {
		return err
	}
	return validateEmbeddingSpec(request.GetSpec())
}

// ValidateEmbeddingTargetObjects accepts a refusal with nothing else, or the
// objects a generation needs: a column type, an owned table exactly when the
// layout owns one, a source reference only from an owned table, an index
// exactly when the specification declares an index method, and extensions
// named once each.
func ValidateEmbeddingTargetObjects(description *protocol.Description, request *protocol.DescribeEmbeddingTargetRequest, result *protocol.EmbeddingTargetObjects) error {
	if err := ValidateDescribeEmbeddingTargetRequest(description, request); err != nil {
		return err
	}
	if result.GetRefusal() != nil {
		return validateRefusedTargetObjects(result)
	}
	target := request.GetSpec().GetTarget()
	if !embeddingName(result.GetColumnType()) {
		return fmt.Errorf("%w: target objects need a column type", ErrProtocol)
	}
	owned := target.GetLayout() == protocol.EmbeddingTarget_OWN_TABLE
	if result.GetOwnsTable() != owned || result.GetReferencesSource() && !owned {
		return fmt.Errorf("%w: target objects disagree with the layout about the table", ErrProtocol)
	}
	declared := target.GetIndexMethod() != ""
	if result.GetHasIndex() != declared || declared != (result.GetIndexMethod() != "") ||
		!declared && result.GetIndexOperator() != "" || !embeddingTexts(result.GetIndexMethod(), result.GetIndexOperator()) {
		return fmt.Errorf("%w: target objects disagree with the specification about the index", ErrProtocol)
	}
	extensions := result.GetRequiredExtensions()
	for i, extension := range extensions {
		if !embeddingName(extension) || slices.Contains(extensions[:i], extension) {
			return fmt.Errorf("%w: an empty, invalid, or repeated required extension", ErrProtocol)
		}
	}
	return nil
}

// validateRefusedTargetObjects accepts a refusal that carries no object.
func validateRefusedTargetObjects(result *protocol.EmbeddingTargetObjects) error {
	if result.GetColumnType() != "" || result.GetOwnsTable() || result.GetReferencesSource() || result.GetHasIndex() ||
		result.GetIndexMethod() != "" || result.GetIndexOperator() != "" || len(result.GetRequiredExtensions()) != 0 {
		return fmt.Errorf("%w: a refused target description carries objects", ErrProtocol)
	}
	return validateEmbeddingRefusal(result.GetRefusal())
}

// ValidateResolveEmbeddingRelationsRequest checks the session and between one
// and MaxEmbeddingRows table references.
func ValidateResolveEmbeddingRelationsRequest(request *protocol.ResolveEmbeddingRelationsRequest) error {
	if err := validateEmbeddingSession(request.GetSessionId(), nil); err != nil {
		return err
	}
	relations := request.GetRelations()
	if len(relations) == 0 || len(relations) > MaxEmbeddingRows {
		return fmt.Errorf("%w: a relation resolution names between 1 and %d relations", ErrProtocol, MaxEmbeddingRows)
	}
	for _, relation := range relations {
		if err := validateEmbeddingTable(relation); err != nil {
			return err
		}
	}
	return nil
}

// ValidateEmbeddingRelations requires one table reference per requested
// relation, in order.
func ValidateEmbeddingRelations(request *protocol.ResolveEmbeddingRelationsRequest, result *protocol.EmbeddingRelations) error {
	if err := ValidateResolveEmbeddingRelationsRequest(request); err != nil {
		return err
	}
	if len(result.GetRelations()) != len(request.GetRelations()) {
		return fmt.Errorf("%w: resolved relations do not answer the request", ErrProtocol)
	}
	for _, relation := range result.GetRelations() {
		if err := validateEmbeddingTable(relation); err != nil {
			return err
		}
	}
	return nil
}

// ValidateMeasureEmbeddingRequest checks the session and the specification.
func ValidateMeasureEmbeddingRequest(request *protocol.MeasureEmbeddingRequest) error {
	if err := validateEmbeddingSession(request.GetSessionId(), nil); err != nil {
		return err
	}
	return validateEmbeddingSpec(request.GetSpec())
}

// ValidateEmbeddingMeasurement refuses a column without its table, a count
// of a source that is not there or one that is negative, a count where none
// was taken, a capability that is not a name, and a refusal that is not one
// line.
func ValidateEmbeddingMeasurement(request *protocol.MeasureEmbeddingRequest, result *protocol.EmbeddingMeasurement) error {
	if err := ValidateMeasureEmbeddingRequest(request); err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("%w: embedding measurement is missing", ErrProtocol)
	}
	if result.GetTargetColumnExists() && !result.GetTargetTableExists() {
		return fmt.Errorf("%w: a measured column without its table", ErrProtocol)
	}
	counted := result.GetRowsCounted()
	if counted && !result.GetSourceExists() || result.GetRows() < 0 || !counted && result.GetRows() != 0 {
		return fmt.Errorf("%w: a measured row count disagrees with the source", ErrProtocol)
	}
	for name := range result.GetCapabilities() {
		if !validName(name) {
			return fmt.Errorf("%w: a measured capability is not a name", ErrProtocol)
		}
	}
	for _, refusal := range result.GetRefusals() {
		if !diagnosticLine(refusal) {
			return fmt.Errorf("%w: a measured refusal is one line of text", ErrProtocol)
		}
	}
	return nil
}

// ValidatePrepareEmbeddingTargetRequest checks the session, the request ID,
// the specification and a known mode.
func ValidatePrepareEmbeddingTargetRequest(request *protocol.PrepareEmbeddingTargetRequest) error {
	requestID := request.GetRequestId()
	if err := validateEmbeddingSession(request.GetSessionId(), &requestID); err != nil {
		return err
	}
	mode := request.GetMode()
	if _, known := protocol.PrepareEmbeddingTargetRequest_Mode_name[int32(mode)]; !known || mode == protocol.PrepareEmbeddingTargetRequest_MODE_UNSPECIFIED {
		return fmt.Errorf("%w: a target preparation needs a known mode", ErrProtocol)
	}
	return validateEmbeddingSpec(request.GetSpec())
}

// ValidatePreparedEmbeddingTarget checks the reply to a target preparation,
// which compares nothing and so cannot conflict.
func ValidatePreparedEmbeddingTarget(request *protocol.PrepareEmbeddingTargetRequest, result *protocol.EmbeddingWriteResult) error {
	if err := ValidatePrepareEmbeddingTargetRequest(request); err != nil {
		return err
	}
	return validateEmbeddingWriteResult(request.GetRequestId(), writeShape{}, result)
}

// ValidateBuildEmbeddingIndexRequest checks the session, the request ID and
// the specification.
func ValidateBuildEmbeddingIndexRequest(request *protocol.BuildEmbeddingIndexRequest) error {
	requestID := request.GetRequestId()
	if err := validateEmbeddingSession(request.GetSessionId(), &requestID); err != nil {
		return err
	}
	return validateEmbeddingSpec(request.GetSpec())
}

// ValidateBuiltEmbeddingIndex checks the reply to an index build. An APPLIED
// reply says what the build did: NOT_DECLARED exactly when the specification
// declares no index method, and a known outcome otherwise.
func ValidateBuiltEmbeddingIndex(request *protocol.BuildEmbeddingIndexRequest, result *protocol.EmbeddingWriteResult) error {
	if err := ValidateBuildEmbeddingIndexRequest(request); err != nil {
		return err
	}
	if err := validateEmbeddingWriteResult(request.GetRequestId(), writeShape{payload: payloadIndex}, result); err != nil {
		return err
	}
	if result.GetOutcome() != protocol.EmbeddingWriteResult_APPLIED {
		return nil
	}
	outcome := result.GetIndexOutcome()
	declared := request.GetSpec().GetTarget().GetIndexMethod() != ""
	if _, known := protocol.EmbeddingWriteResult_IndexOutcome_name[int32(outcome)]; !known ||
		declared == (outcome == protocol.EmbeddingWriteResult_NOT_DECLARED) {
		return fmt.Errorf("%w: an index outcome disagrees with the specification", ErrProtocol)
	}
	return nil
}

// ValidateReadEmbeddingStructureRequest checks the session and the location.
func ValidateReadEmbeddingStructureRequest(request *protocol.ReadEmbeddingStructureRequest) error {
	if err := validateEmbeddingSession(request.GetSessionId(), nil); err != nil {
		return err
	}
	return validateEmbeddingLocation(request.GetLocation())
}

// ValidateEmbeddingStructure refuses a column or an index without its table,
// a valid index that is not there, a column type exactly when there is no
// column, an index without a method, a count the request did not ask for or
// one it asked for and did not get, a negative count, and search settings
// that are not one line.
func ValidateEmbeddingStructure(request *protocol.ReadEmbeddingStructureRequest, result *protocol.EmbeddingStructure) error {
	if err := ValidateReadEmbeddingStructureRequest(request); err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("%w: embedding structure is missing", ErrProtocol)
	}
	table := result.GetTableExists()
	if result.GetColumnExists() && !table || result.GetIndexExists() && !table || result.GetIndexValid() && !result.GetIndexExists() {
		return fmt.Errorf("%w: embedding structure reports an object without what holds it", ErrProtocol)
	}
	if result.GetColumnExists() != (result.GetColumnType() != "") || result.GetIndexExists() != (result.GetIndexMethod() != "") ||
		!result.GetIndexExists() && result.GetIndexOperator() != "" ||
		!embeddingTexts(result.GetColumnType(), result.GetIndexMethod(), result.GetIndexOperator()) {
		return fmt.Errorf("%w: embedding structure describes an object it does not report", ErrProtocol)
	}
	if result.GetRowsCounted() != request.GetCountRows() || result.GetRows() < 0 || !result.GetRowsCounted() && result.GetRows() != 0 {
		return fmt.Errorf("%w: embedding structure answers another count than the request asked for", ErrProtocol)
	}
	if settings := result.GetSearchSettings(); settings != "" && !diagnosticLine(settings) {
		return fmt.Errorf("%w: search settings are one line of text", ErrProtocol)
	}
	return nil
}

// ValidateDropEmbeddingGenerationRequest checks the session, the request ID,
// the location and a known storage. Dropping a table needs the ownership
// mark the table is checked against.
func ValidateDropEmbeddingGenerationRequest(request *protocol.DropEmbeddingGenerationRequest) error {
	requestID := request.GetRequestId()
	if err := validateEmbeddingSession(request.GetSessionId(), &requestID); err != nil {
		return err
	}
	if err := validateEmbeddingLocation(request.GetLocation()); err != nil {
		return err
	}
	storage := request.GetStorage()
	if _, known := protocol.DropEmbeddingGenerationRequest_Storage_name[int32(storage)]; !known ||
		storage == protocol.DropEmbeddingGenerationRequest_STORAGE_UNSPECIFIED {
		return fmt.Errorf("%w: a generation drop needs a known storage", ErrProtocol)
	}
	if storage == protocol.DropEmbeddingGenerationRequest_TABLE && request.GetLocation().GetNames().GetOwnershipMark() == "" {
		return fmt.Errorf("%w: a table drop needs the ownership mark", ErrProtocol)
	}
	return nil
}

// ValidateDroppedEmbeddingGeneration checks the reply to a generation drop,
// which compares nothing and so cannot conflict.
func ValidateDroppedEmbeddingGeneration(request *protocol.DropEmbeddingGenerationRequest, result *protocol.EmbeddingWriteResult) error {
	if err := ValidateDropEmbeddingGenerationRequest(request); err != nil {
		return err
	}
	return validateEmbeddingWriteResult(request.GetRequestId(), writeShape{}, result)
}

package ydbsource

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/ydbchangefeed"
)

// encodedKinds are the owner's models its Go annotation encoder writes: its
// standalone objects, a table's changefeeds, column families, partitioning
// and column storage, and an index's partitioning and vector settings.
var encodedKinds = []schemaext.Kind{
	ydbschema.ChangefeedKind, ydbschema.ColumnFamiliesKind,
	ydbschema.TablePartitioningKind, ydbschema.ColumnStoreKind, ydbschema.IndexPartitioningKind, ydbschema.VectorIndexKind,
	ydbcoordination.Kind, ydbstreaming.Kind, ydbworkload.PoolKind, ydbworkload.ClassifierKind,
	ydbsecret.Kind, ydbtopic.Kind, ydbexternal.SourceKind, ydbexternal.TableKind,
	ydbreplication.ReplicationKind, ydbreplication.TransferKind,
}

// annotationEncoder writes the YDB owner's objects of one schema as the
// directives its Go annotation extension reads back.
type annotationEncoder struct{}

// EncodeAnnotations writes:
//
//   - the limits of the namespaces the schema does not describe, when the
//     export is for YDB or names no target: another target never holds these
//     objects, so a limit there says nothing a reader could use;
//   - after the views, each topic with its consumers, each async replication
//     with its items, each transfer, each secret by the variable its value
//     comes from, each external data source and then each external table,
//     and each streaming query;
//   - after the default privileges, each resource pool and classifier, and
//     then each coordination node;
//   - beside each table, its column families and then its changefeeds with
//     their consumers;
//   - the settings of a table and of an index as attributes of their
//     directives (see slotSettings);
//   - for a YDB export, the Go type of each field whose type YDB names (see
//     fieldTypes).
//
// Each family is written in the order of the objects' identities. A value
// that is not a valid declaration, a changefeed whose table the schema does
// not declare, a disabled changefeed and one that keeps a replication's
// binding, a setting on a declaration with no spelling for it, and a
// coverage record a source cannot carry are refused rather than left out.
func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	limits, err := exportLimits(request.Coverage)
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	var result annotation.EncodeResult
	if request.Target == "" || request.Target == platform.YDB {
		for _, limit := range limits {
			result.Limits = append(result.Limits, annotation.EncodedLimit{Kind: string(limit.Kind), Name: limit.Name, Reason: limit.Reason, Provenance: limit.Provenance})
		}
	}
	families, attributes, err := slotSettings(request.Slots)
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	result.Attributes = attributes
	if result.FieldTypes, err = fieldTypes(request); err != nil {
		return annotation.EncodeResult{}, err
	}
	var sorted objectDirectives
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		if err := sorted.add(object, request.Tables); err != nil {
			return annotation.EncodeResult{}, err
		}
	}
	result.Directives = sorted.inOrder(families)
	return result, nil
}

// objectDirectives are the directives of the schema's objects, by where and
// in which group the source writes them.
type objectDirectives struct {
	topics, replications, transfers, secrets, sources, externalTables, streaming []annotation.EncodedDirective
	workload, coordination, changefeeds                                          []annotation.EncodedDirective
}

// inOrder is every directive in the order the source writes them: the
// objects after the views, those after the default privileges, then the
// column families and the changefeeds beside their tables.
func (d objectDirectives) inOrder(families []annotation.EncodedDirective) []annotation.EncodedDirective {
	var all []annotation.EncodedDirective
	for _, group := range [][]annotation.EncodedDirective{d.topics, d.replications, d.transfers, d.secrets, d.sources,
		d.externalTables, d.streaming, d.workload, d.coordination, families, d.changefeeds} {
		all = append(all, group...)
	}
	return all
}

// add validates object and records its directives in their group.
func (d *objectDirectives) add(object schemaext.Object, tables annotation.Tables) error {
	ref := object.Ref
	switch value := object.Value.(type) {
	case *ydbsecret.Desired:
		return d.addSecret(ref, value)
	case *ydbtopic.Desired:
		if err := firstError(ydbtopic.ValidateIdentity(ref), func() error { return ydbtopic.Validate(value.Spec) }); err != nil {
			return err
		}
		d.topics = append(d.topics, encodeTopic(ref.Schema.Source, ref.Name.Source, value.Spec)...)
	case *ydbreplication.DesiredReplication:
		if err := firstError(ydbreplication.ValidateIdentity(ref), value.Validate); err != nil {
			return err
		}
		d.replications = append(d.replications, encodeReplication(ref.Schema.Source, ref.Name.Source, value.Spec)...)
	case *ydbreplication.DesiredTransfer:
		if err := firstError(ydbreplication.ValidateIdentity(ref), value.Validate); err != nil {
			return err
		}
		d.transfers = append(d.transfers, encodeTransfer(ref.Schema.Source, ref.Name.Source, value.Spec))
	case *ydbexternal.DesiredSource:
		if err := firstError(ydbexternal.ValidateIdentity(ref), value.Validate); err != nil {
			return err
		}
		d.sources = append(d.sources, encodeExternalSource(ref.Schema.Source, ref.Name.Source, value.Spec))
	case *ydbexternal.DesiredTable:
		if err := firstError(ydbexternal.ValidateIdentity(ref), value.Validate); err != nil {
			return err
		}
		d.externalTables = append(d.externalTables, encodeExternalTable(ref.Schema.Source, ref.Name.Source, value.Spec))
	default:
		return d.addOther(object, tables)
	}
	return nil
}

// addOther records a workload object, a streaming query, a coordination
// node or a changefeed, and refuses a value of any other type.
func (d *objectDirectives) addOther(object schemaext.Object, tables annotation.Tables) error {
	ref := object.Ref
	switch value := object.Value.(type) {
	case *ydbworkload.DesiredPool:
		if err := ydbworkload.ValidatePoolRef(ref, value.Spec); err != nil {
			return err
		}
		d.workload = append(d.workload, encodeResourcePool(ref.Name.Source, value.Spec))
	case *ydbworkload.DesiredClassifier:
		if err := firstError(ydbworkload.ValidateIdentity(ref, ydbworkload.ClassifierKind),
			func() error { return ydbworkload.ValidateClassifier(value.Spec) }); err != nil {
			return err
		}
		d.workload = append(d.workload, encodeClassifier(ref.Name.Source, value.Spec))
	case *ydbstreaming.Desired:
		if err := firstError(ydbstreaming.ValidateIdentity(ref), func() error { return ydbstreaming.Validate(value.Spec) }); err != nil {
			return err
		}
		d.streaming = append(d.streaming, encodeStreamingQuery(ref.Schema.Source, ref.Name.Source, value))
	case *ydbcoordination.Desired:
		if err := firstError(ydbcoordination.ValidateRef(ref), func() error { return ydbcoordination.Validate(value.Spec) }); err != nil {
			return err
		}
		d.coordination = append(d.coordination, encodeCoordinationNode(ref.Schema.Source, ref.Name.Source, value.Spec))
	case *ydbschema.DesiredChangefeed:
		directives, err := encodeChangefeed(ref, value, tables)
		if err != nil {
			return err
		}
		d.changefeeds = append(d.changefeeds, directives...)
	default:
		return fmt.Errorf("%w: Go annotations cannot represent feature object %s with value %T", ptaherr.ErrUnsupportedFeature, ref, object.Value)
	}
	return nil
}

// addSecret records a secret by the variable its value comes from, and
// never a value, which no model holds. A declaration that names no variable
// is written with the default one for its path, which is what it selects.
func (d *objectDirectives) addSecret(ref objectidentity.ID, secret *ydbsecret.Desired) error {
	if err := firstError(ydbsecret.ValidateIdentity(ref), secret.Validate); err != nil {
		return err
	}
	var attrs encodedAttrs
	attrs.add(ydbsecret.AttributeName, ref.Name.Source)
	attrs.text(ydbsecret.AttributeSchema, ref.Schema.Source)
	attrs.add(ydbsecret.AttributeValueEnv, secret.Variable(ref))
	d.secrets = append(d.secrets, attrs.directive(directiveSecret, annotation.AfterViews))
	return nil
}

// firstError is identity's error, or else the error of validate.
func firstError(identity error, validate func() error) error {
	if identity != nil {
		return identity
	}
	return validate()
}

// encodedAttrs are a directive's attributes in the order it writes them.
type encodedAttrs []annotation.EncodedAttribute

// add writes the attribute name with value.
func (a *encodedAttrs) add(name, value string) {
	*a = append(*a, annotation.EncodedAttribute{Name: name, Value: value})
}

// text writes the attribute name where value is not empty.
func (a *encodedAttrs) text(name, value string) {
	if value != "" {
		a.add(name, value)
	}
}

// count writes the attribute name where value is not zero.
func (a *encodedAttrs) count(name string, value uint64) {
	if value != 0 {
		a.add(name, strconv.FormatUint(value, 10))
	}
}

// flagValue is the value of a flag attribute: true where the flag is set, and
// empty, which [encodedAttrs.text] leaves out, where it is not.
var flagValue = map[bool]string{true: "true"}

// directive is a as the directive name at placement.
func (a encodedAttrs) directive(name string, placement annotation.Placement) annotation.EncodedDirective {
	return annotation.EncodedDirective{Directive: name, Placement: placement, Attributes: a}
}

// encodeTopic are a topic in the directory schema and one directive for
// each of its consumers, each naming only what differs from the zero value.
func encodeTopic(schema, name string, spec ydbtopic.Spec) []annotation.EncodedDirective {
	var topic encodedAttrs
	topic.add(ydbtopic.AttributeName, name)
	topic.text(ydbtopic.AttributeSchema, schema)
	topic.count(ydbtopic.AttributeMinActivePartitions, spec.MinActivePartitions)
	topic.count(ydbtopic.AttributeMaxActivePartitions, spec.MaxActivePartitions)
	topic.text(ydbtopic.AttributeStrategy, spec.AutoPartitioningStrategy)
	topic.count(ydbtopic.AttributeUpUtilizationPercent, uint64(spec.AutoPartitioningUpUtilizationPercent))
	topic.count(ydbtopic.AttributeDownUtilizationPercent, uint64(spec.AutoPartitioningDownUtilizationPercent))
	topic.text(ydbtopic.AttributeStabilizationWindow, spec.AutoPartitioningStabilizationWindow)
	topic.text(ydbtopic.AttributeRetentionPeriod, spec.RetentionPeriod)
	topic.count(ydbtopic.AttributeWriteSpeed, spec.PartitionWriteSpeedBytesPerSecond)
	topic.count(ydbtopic.AttributeWriteBurst, spec.PartitionWriteBurstBytes)
	topic.text(ydbtopic.AttributeSupportedCodecs, strings.Join(spec.SupportedCodecs, ","))
	directives := []annotation.EncodedDirective{topic.directive(directiveTopic, annotation.AfterViews)}
	for _, consumer := range spec.Consumers {
		var attrs encodedAttrs
		attrs.add(ydbtopic.AttributeName, consumer.Name)
		attrs.add(ydbtopic.AttributeTopic, name)
		attrs.text(ydbtopic.AttributeSchema, schema)
		attrs.text(ydbtopic.AttributeImportant, flagValue[consumer.Important])
		attrs.text(ydbtopic.AttributeReadFrom, consumer.ReadFrom)
		attrs.text(ydbtopic.AttributeSupportedCodecs, strings.Join(consumer.SupportedCodecs, ","))
		attrs.text(ydbtopic.AttributeAvailabilityPeriod, consumer.AvailabilityPeriod)
		directives = append(directives, attrs.directive(directiveTopicConsumer, annotation.AfterViews))
	}
	return directives
}

// connection writes a replication's or a transfer's connection, each
// attribute naming only what the connection names.
func (a *encodedAttrs) connection(connection ydbreplication.Connection) {
	a.text(ydbreplication.AttributeConnectionString, connection.ConnectionString)
	a.text(ydbreplication.AttributeTokenSecretName, connection.TokenSecretName)
	a.text(ydbreplication.AttributeTokenSecretPath, connection.TokenSecretPath)
	a.text(ydbreplication.AttributeUser, connection.User)
	a.text(ydbreplication.AttributePasswordSecretName, connection.PasswordSecretName)
	a.text(ydbreplication.AttributePasswordSecretPath, connection.PasswordSecretPath)
}

// encodeReplication are an async replication in the directory schema
// and one directive for each of its items.
func encodeReplication(schema, name string, spec ydbreplication.ReplicationSpec) []annotation.EncodedDirective {
	var replication encodedAttrs
	replication.add(ydbreplication.AttributeName, name)
	replication.text(ydbreplication.AttributeSchema, schema)
	replication.connection(spec.Connection)
	replication.text(ydbreplication.AttributeConsistencyLevel, spec.ConsistencyLevel)
	replication.text(ydbreplication.AttributeCommitInterval, spec.CommitInterval)
	directives := []annotation.EncodedDirective{replication.directive(directiveAsyncReplication, annotation.AfterViews)}
	for _, item := range spec.Items {
		var attrs encodedAttrs
		attrs.add(ydbreplication.AttributeReplication, name)
		attrs.text(ydbreplication.AttributeSchema, schema)
		attrs.add(ydbreplication.AttributeSource, item.Source)
		attrs.add(ydbreplication.AttributeTarget, item.Target)
		directives = append(directives, attrs.directive(directiveAsyncReplicationFor, annotation.AfterViews))
	}
	return directives
}

// encodeTransfer is a transfer in the directory schema.
func encodeTransfer(schema, name string, spec ydbreplication.TransferSpec) annotation.EncodedDirective {
	var attrs encodedAttrs
	attrs.add(ydbreplication.AttributeName, name)
	attrs.text(ydbreplication.AttributeSchema, schema)
	attrs.connection(spec.Connection)
	attrs.add(ydbreplication.AttributeSource, spec.Source)
	attrs.add(ydbreplication.AttributeTarget, spec.Target)
	attrs.add(ydbreplication.AttributeUsing, spec.Lambda)
	attrs.text(ydbreplication.AttributeConsumer, spec.Consumer)
	attrs.count(ydbreplication.AttributeBatchSizeBytes, spec.BatchSizeBytes)
	attrs.text(ydbreplication.AttributeFlushInterval, spec.FlushInterval)
	return attrs.directive(directiveTransfer, annotation.AfterViews)
}

// encodeExternalSource is an external data source in the directory
// schema. A credential is an option naming a secret, never a value.
func encodeExternalSource(schema, name string, source ydbexternal.DataSource) annotation.EncodedDirective {
	var attrs encodedAttrs
	attrs.add(ydbexternal.AttributeName, name)
	attrs.text(ydbexternal.AttributeSchema, schema)
	attrs.add(ydbexternal.AttributeSourceType, source.SourceType)
	attrs.text(ydbexternal.AttributeLocation, source.Location)
	attrs.add(ydbexternal.AttributeAuthMethod, source.AuthMethod)
	if len(source.Options) > 0 {
		attrs.add(ydbexternal.AttributeOptions, ydbexternal.FormatOptions(source.Options))
	}
	return attrs.directive(directiveExternalDataSource, annotation.AfterViews)
}

// encodeExternalTable is an external table in the directory schema.
func encodeExternalTable(schema, name string, table ydbexternal.Table) annotation.EncodedDirective {
	var attrs encodedAttrs
	attrs.add(ydbexternal.AttributeName, name)
	attrs.text(ydbexternal.AttributeSchema, schema)
	attrs.add(ydbexternal.AttributeDataSource, table.DataSource)
	attrs.add(ydbexternal.AttributeLocation, table.Location)
	attrs.add(ydbexternal.AttributeColumns, ydbexternal.FormatColumns(table.Columns))
	if len(table.Options) > 0 {
		attrs.add(ydbexternal.AttributeOptions, ydbexternal.FormatOptions(table.Options))
	}
	return attrs.directive(directiveExternalTable, annotation.AfterViews)
}

// encodeStreamingQuery is a streaming query in the directory schema.
func encodeStreamingQuery(schema, name string, query *ydbstreaming.Desired) annotation.EncodedDirective {
	var attrs encodedAttrs
	attrs.add("name", name)
	attrs.text("schema", schema)
	attrs.add("text", query.Spec.Text)
	attrs.add("run", strconv.FormatBool(ydbstreaming.Running(query.Spec)))
	attrs.add("resource_pool", ydbstreaming.Pool(query.Spec))
	attrs.text("allow_state_reset", flagValue[query.AllowStateReset])
	return attrs.directive(directiveStreamingQuery, annotation.AfterViews)
}

// encodeResourcePool is a resource pool with the settings it states.
func encodeResourcePool(name string, spec ydbworkload.PoolSpec) annotation.EncodedDirective {
	var attrs encodedAttrs
	attrs.add(ydbworkload.AttributeName, name)
	integer := func(name string, value *int32) {
		if value != nil {
			attrs.add(name, strconv.FormatInt(int64(*value), 10))
		}
	}
	fraction := func(name string, value *float64) {
		if value != nil {
			attrs.add(name, strconv.FormatFloat(*value, 'f', -1, 64))
		}
	}
	integer(ydbworkload.AttributeConcurrentQueryLimit, spec.ConcurrentQueryLimit)
	integer(ydbworkload.AttributeQueueSize, spec.QueueSize)
	fraction(ydbworkload.AttributeDatabaseLoadCPUThreshold, spec.DatabaseLoadCPUThreshold)
	fraction(ydbworkload.AttributeQueryMemoryLimitPercentPerNode, spec.QueryMemoryLimitPercentPerNode)
	fraction(ydbworkload.AttributeQueryCPULimitPercentPerNode, spec.QueryCPULimitPercentPerNode)
	fraction(ydbworkload.AttributeTotalCPULimitPercentPerNode, spec.TotalCPULimitPercentPerNode)
	fraction(ydbworkload.AttributeResourceWeight, spec.ResourceWeight)
	return attrs.directive(directiveResourcePool, annotation.AfterPrivileges)
}

// encodeClassifier is a resource pool classifier.
func encodeClassifier(name string, spec ydbworkload.ClassifierSpec) annotation.EncodedDirective {
	var attrs encodedAttrs
	attrs.add(ydbworkload.AttributeName, name)
	attrs.add(ydbworkload.AttributeResourcePool, spec.ResourcePool)
	attrs.text(ydbworkload.AttributeMemberName, spec.MemberName)
	attrs.add(ydbworkload.AttributeRank, strconv.FormatInt(spec.Rank, 10))
	return attrs.directive(directiveClassifier, annotation.AfterPrivileges)
}

// encodeCoordinationNode is a coordination node with the settings it was
// given; a setting left unset takes YDB's default.
func encodeCoordinationNode(schema, name string, spec ydbcoordination.Spec) annotation.EncodedDirective {
	var attrs encodedAttrs
	attrs.add("name", name)
	attrs.text("schema", schema)
	for _, setting := range ydbcoordination.Attributes(spec) {
		attrs.add(setting[0], setting[1])
	}
	return attrs.directive(directiveCoordinationNode, annotation.AfterPrivileges)
}

// encodeChangefeed are a changefeed and its consumers beside each
// declared table it belongs to. What a Go annotation cannot preserve is
// refused: a disabled changefeed, since no statement disables one, and the
// binding a replication keeps.
func encodeChangefeed(ref objectidentity.ID, feed *ydbschema.DesiredChangefeed, tables annotation.Tables) ([]annotation.EncodedDirective, error) {
	if err := ydbschema.ValidateChangefeed(feed.Spec); err != nil {
		return nil, err
	}
	if feed.Spec.Disabled {
		return nil, fmt.Errorf("%w: Go annotations cannot preserve disabled changefeed %s", ptaherr.ErrUnsupportedFeature, ref)
	}
	if feed.RetainedReplication != nil {
		return nil, fmt.Errorf("%w: Go annotations cannot preserve the retained replication binding of changefeed %s", ptaherr.ErrUnsupportedFeature, ref)
	}
	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.YDB))
	parent := builder.TableParts(ref.Schema.Source, ref.Parent.Source).Key()
	var declared []annotation.Table
	for _, table := range tables {
		if builder.TableParts(table.Schema, table.Name).Key() == parent {
			declared = append(declared, table)
		}
	}
	if len(declared) == 0 {
		return nil, fmt.Errorf("%w: feature object %s has no declared parent table", ptaherr.ErrInvalidSchemaDiff, ref)
	}
	if ref.Key() != ydbschema.ChangefeedRef(ref.Schema.Source, ref.Parent.Source, feed.Spec.Name).Key() {
		return nil, fmt.Errorf("%w: changefeed name disagrees with its reference", ptaherr.ErrInvalidSchemaDiff)
	}
	var directives []annotation.EncodedDirective
	for _, table := range declared {
		for _, directive := range changefeedAndConsumers(feed.Spec) {
			directive.Table = &table
			directives = append(directives, directive)
		}
	}
	return directives, nil
}

// changefeedAndConsumers is a changefeed and its consumers, without their table. A
// disabled changefeed would be written as an enabled one: no annotation
// declares one disabled, because no statement disables one.
func changefeedAndConsumers(changefeed ydbschema.ChangefeedSpec) []annotation.EncodedDirective {
	var attrs encodedAttrs
	attrs.add(ydbchangefeed.AttributeName, changefeed.Name)
	attrs.add(ydbchangefeed.AttributeMode, changefeed.Mode)
	attrs.add(ydbchangefeed.AttributeFormat, changefeed.Format)
	attrs.text(ydbchangefeed.AttributeVirtualTimestamps, flagValue[changefeed.VirtualTimestamps])
	attrs.text(ydbchangefeed.AttributeResolvedTimestamps, changefeed.ResolvedTimestamps)
	attrs.text(ydbchangefeed.AttributeInitialScan, flagValue[changefeed.InitialScan])
	attrs.text(ydbchangefeed.AttributeUserSIDs, flagValue[changefeed.UserSIDs])
	attrs.text(ydbchangefeed.AttributeSchemaChanges, flagValue[changefeed.SchemaChanges])
	attrs.count(ydbchangefeed.AttributeTopicMinActivePartitions, changefeed.TopicMinActivePartitions)
	attrs.text(ydbchangefeed.AttributeTopicAutoPartitioning, flagValue[changefeed.TopicAutoPartitioning])
	attrs.text(ydbchangefeed.AttributeRetentionPeriod, changefeed.RetentionPeriod)
	directives := []annotation.EncodedDirective{attrs.directive(directiveChangefeed, annotation.AfterTable)}
	for _, consumer := range changefeed.Consumers {
		var consumerAttrs encodedAttrs
		consumerAttrs.add(ydbchangefeed.AttributeChangefeed, changefeed.Name)
		consumerAttrs.add(ydbchangefeed.AttributeName, consumer.Name)
		consumerAttrs.text(ydbchangefeed.AttributeImportant, flagValue[consumer.Important])
		consumerAttrs.text(ydbchangefeed.AttributeReadFrom, consumer.ReadFrom)
		consumerAttrs.text(ydbchangefeed.AttributeSupportedCodecs, strings.Join(consumer.SupportedCodecs, ","))
		consumerAttrs.text(ydbchangefeed.AttributeAvailabilityPeriod, consumer.AvailabilityPeriod)
		directives = append(directives, consumerAttrs.directive(directiveChangefeedConsumer, annotation.AfterTable))
	}
	return directives
}

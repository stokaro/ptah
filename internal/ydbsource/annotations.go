package ydbsource

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
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
	"ptah.run/internal/ydbfamily"
	"ptah.run/internal/ydbpartition"
)

// The YDB directives of the Go annotation frontend.
const (
	directiveChangefeed          = "ptah:schema:changefeed"
	directiveChangefeedConsumer  = "ptah:schema:changefeed:consumer"
	directiveColumnFamily        = "ptah:schema:columnfamily"
	directiveCoordinationNode    = "ptah:schema:coordinationnode"
	directiveTopic               = "ptah:schema:topic"
	directiveTopicConsumer       = "ptah:schema:topic:consumer"
	directiveResourcePool        = "ptah:schema:resourcepool"
	directiveClassifier          = "ptah:schema:resourcepool:classifier"
	directiveSecret              = "ptah:schema:secret" // #nosec G101 -- a directive name, not a credential
	directiveStreamingQuery      = "ptah:schema:streamingquery"
	directiveExternalDataSource  = "ptah:schema:externaldatasource"
	directiveExternalTable       = "ptah:schema:externaltable"
	directiveAsyncReplication    = "ptah:schema:async_replication"
	directiveAsyncReplicationFor = "ptah:schema:async_replication:item"
	directiveTransfer            = "ptah:schema:transfer"
)

// Annotations is the YDB owner's contribution to the Go annotation frontend:
// the directives of its standalone objects, of a table's changefeeds and
// column families, and of the consumers and items those declare in the same
// file; the settings a table and an index declare in the frontend's own
// directives; the not-described kinds that leave a namespace of those objects
// unmanaged; and the claim that a Go annotation source describes each of
// them, and a table's TTL, completely.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner:      ydbschema.Owner,
		Directives: directives(),
		Attributes: attributes(),
		Kinds: []schemaext.Kind{
			ydbschema.ChangefeedKind, ydbschema.ColumnFamiliesKind, ydbschema.TTLKind,
			ydbschema.TablePartitioningKind, ydbschema.ColumnStoreKind, ydbschema.IndexPartitioningKind,
			ydbschema.VectorIndexKind,
			ydbcoordination.Kind, ydbstreaming.Kind, ydbworkload.PoolKind, ydbworkload.ClassifierKind,
			ydbsecret.Kind, ydbtopic.Kind, ydbexternal.SourceKind, ydbexternal.TableKind,
			ydbreplication.ReplicationKind, ydbreplication.TransferKind,
		},
		File:     func() annotation.FileDecoder { return &fileDecoder{} },
		Limits:   limitTokens(),
		Coverage: annotationCoverage,
	}
}

// limitTokens are the not-described kinds a source writes for the namespaces
// this package enrolls.
func limitTokens() []string {
	tokens := make([]string, 0, len(sourceKinds))
	for _, family := range sourceKinds {
		tokens = append(tokens, family.token)
	}
	return tokens
}

// annotationCoverage is the claim of a Go annotation source that wrote
// limits: each namespace it can declare, less what the limits leave
// unmanaged.
func annotationCoverage(limits []coverage.Object) (schemaext.Coverage, error) {
	var recorded Limits
	for _, limit := range limits {
		if !recorded.Add(string(limit.Kind), limit.Name) {
			return schemaext.Coverage{}, fmt.Errorf("YDB reads no not-described kind %q", limit.Kind)
		}
	}
	return Coverage(recorded)
}

// fileDecoder reads the YDB declarations of one file. A standalone object is
// declared as soon as it is read, so a second declaration of it is refused
// where it is written. A changefeed, a column family, a topic and a
// replication wait for the end of the file: a changefeed and a family belong
// to a table the file may declare later, and the others gather consumers and
// items the file may declare later.
type fileDecoder struct {
	// objects are the objects the file declared, so a family's own refusal
	// of a second declaration names it.
	objects        schemaext.Objects
	changefeeds    []pendingChangefeed
	consumers      []pendingConsumer
	families       []pendingFamily
	topics         []pendingTopic
	topicConsumers []pendingTopicConsumer
	replications   []pendingReplication
	items          []pendingItem
}

type pendingChangefeed struct {
	source annotation.Declaration
	spec   ydbschema.ChangefeedSpec
}

type pendingConsumer struct {
	source   annotation.Declaration
	consumer ydbtopic.ConsumerSpec
}

type pendingFamily struct {
	source annotation.Declaration
	spec   ydbschema.ColumnFamily
}

type pendingTopic struct {
	source       annotation.Declaration
	schema, name string
	spec         ydbtopic.Spec
}

type pendingTopicConsumer struct {
	source        annotation.Declaration
	schema, topic string
	consumer      ydbtopic.ConsumerSpec
}

type pendingReplication struct {
	source       annotation.Declaration
	schema, name string
	spec         ydbreplication.ReplicationSpec
}

type pendingItem struct {
	source              annotation.Declaration
	schema, replication string
	item                ydbreplication.Item
}

// Decode reads one declaration.
func (d *fileDecoder) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	kv := declaration.Attributes
	switch declaration.Directive {
	case directiveChangefeed:
		spec, err := ydbchangefeed.ParseDeclaration(kv)
		if err != nil {
			return nil, refusal(err)
		}
		d.changefeeds = append(d.changefeeds, pendingChangefeed{source: declaration, spec: spec})
	case directiveChangefeedConsumer:
		consumer, err := ydbtopic.ParseConsumer(kv)
		if err != nil {
			return nil, refusal(err)
		}
		d.consumers = append(d.consumers, pendingConsumer{source: declaration, consumer: consumer})
	case directiveColumnFamily:
		spec, err := ydbfamily.ParseDeclaration(kv)
		if err != nil {
			return nil, refusal(err)
		}
		d.families = append(d.families, pendingFamily{source: declaration, spec: spec})
	case directiveTopic:
		spec, err := ydbtopic.ParseTopic(kv)
		if err != nil {
			return nil, refusal(err)
		}
		d.topics = append(d.topics, pendingTopic{source: declaration, spec: spec,
			schema: strings.TrimSpace(kv[ydbtopic.AttributeSchema]), name: strings.TrimSpace(kv[ydbtopic.AttributeName])})
	case directiveTopicConsumer:
		consumer, err := ydbtopic.ParseConsumer(kv)
		if err == nil {
			err = ydbtopic.CheckDirectory(kv[ydbtopic.AttributeSchema])
		}
		if err != nil {
			return nil, refusal(err)
		}
		d.topicConsumers = append(d.topicConsumers, pendingTopicConsumer{source: declaration, consumer: consumer,
			schema: strings.TrimSpace(kv[ydbtopic.AttributeSchema]), topic: strings.TrimSpace(kv[ydbtopic.AttributeTopic])})
	case directiveAsyncReplication:
		spec, err := ydbreplication.ParseReplication(kv)
		if err != nil {
			return nil, refusal(err)
		}
		d.replications = append(d.replications, pendingReplication{source: declaration, spec: spec,
			schema: strings.Trim(strings.TrimSpace(kv[ydbreplication.AttributeSchema]), "/"),
			name:   strings.TrimSpace(kv[ydbreplication.AttributeName])})
	case directiveAsyncReplicationFor:
		item, err := ydbreplication.ParseItem(kv)
		if err != nil {
			return nil, refusal(err)
		}
		d.items = append(d.items, pendingItem{source: declaration, item: item,
			schema:      strings.Trim(strings.TrimSpace(kv[ydbreplication.AttributeSchema]), "/"),
			replication: strings.TrimSpace(kv[ydbreplication.AttributeReplication])})
	default:
		return d.decodeObject(declaration)
	}
	return nil, nil
}

// decodeObject reads the declaration of a standalone object, which nothing
// else in the file refers to.
func (d *fileDecoder) decodeObject(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	kv := declaration.Attributes
	var (
		next schemaext.Objects
		err  error
	)
	switch declaration.Directive {
	case directiveCoordinationNode:
		next, err = declareCoordinationNode(d.objects, kv, declaration.Struct)
	case directiveResourcePool:
		next, err = declarePool(d.objects, kv, declaration.Struct)
	case directiveClassifier:
		next, err = declareClassifier(d.objects, kv, declaration.Struct)
	case directiveSecret:
		next, err = declareSecret(d.objects, kv, declaration.Struct)
	case directiveStreamingQuery:
		next, err = declareStreamingQuery(d.objects, kv, declaration.Struct)
	case directiveExternalDataSource:
		next, err = declareExternalDataSource(d.objects, kv, declaration.Struct)
	case directiveExternalTable:
		next, err = declareExternalTable(d.objects, kv, declaration.Struct)
	case directiveTransfer:
		next, err = declareTransfer(d.objects, kv, declaration.Struct)
	default:
		return nil, fmt.Errorf("YDB declares no directive %q", declaration.Directive)
	}
	if err != nil {
		return nil, refusal(err)
	}
	return d.added(next, annotation.Declaration{})
}

// added records next as the file's objects and contributes the ones it holds
// that the file did not declare before. source names the declaration, or is
// zero for the one being decoded.
func (d *fileDecoder) added(next schemaext.Objects, source annotation.Declaration) ([]annotation.Contribution, error) {
	fresh, err := next.Select(func(ref objectidentity.ID) bool {
		_, found, _ := d.objects.Get(ref)
		return !found
	}).All()
	if err != nil {
		return nil, err
	}
	d.objects = next
	contributions := make([]annotation.Contribution, 0, len(fresh))
	for _, object := range fresh {
		contributions = append(contributions, annotation.Contribution{Object: &object, Source: source,
			Label: fmt.Sprintf("YDB object %s", object.Ref)})
	}
	return contributions, nil
}

// Finish declares what waited for the end of the file, in the order the
// frontend once attached them: changefeeds and their consumers, topics and
// theirs, replications and their items, and column families.
func (d *fileDecoder) Finish(tables annotation.Tables) ([]annotation.Contribution, error) {
	var contributions []annotation.Contribution
	for _, finish := range []func(annotation.Tables) ([]annotation.Contribution, error){
		d.finishChangefeeds, d.finishTopics, d.finishReplications, d.finishFamilies,
	} {
		finished, err := finish(tables)
		if err != nil {
			return nil, err
		}
		contributions = append(contributions, finished...)
	}
	return contributions, nil
}

// finishChangefeeds declares each changefeed on its table, then gives each
// changefeed the consumers declared for it. A changefeed of a table the file
// does not declare is refused rather than dropped, because a stream nobody
// gets is a declaration with no effect, and so is a consumer of a changefeed
// the file does not declare.
func (d *fileDecoder) finishChangefeeds(tables annotation.Tables) ([]annotation.Contribution, error) {
	changefeeds := schemaext.Objects{}
	sources := make(map[string]annotation.Declaration)
	for _, pending := range d.changefeeds {
		index, err := tables.Owning(pending.source.Struct, pending.source.Attributes[ydbchangefeed.AttributeTable], "a changefeed")
		if err != nil {
			return nil, placed(pending.source, "", err)
		}
		table := tables[index]
		object := ydbschema.DesiredObject(table.Schema, table.Name, pending.spec)
		changefeeds, err = changefeeds.With(object)
		if errors.Is(err, schemaext.ErrDuplicate) {
			return nil, placed(pending.source, "", fmt.Errorf("table %q declares changefeed %q twice", table.Name, pending.spec.Name))
		}
		if err != nil {
			return nil, placed(pending.source, "", err)
		}
		sources[object.Ref.String()] = pending.source
	}
	for _, pending := range d.consumers {
		written := pending.source.Attributes[ydbchangefeed.AttributeTable]
		index, err := tables.Owning(pending.source.Struct, written, "a changefeed")
		if err != nil {
			return nil, placed(pending.source, "", err)
		}
		table := tables[index]
		changefeed := pending.source.Attributes[ydbchangefeed.AttributeChangefeed]
		object, found, err := changefeeds.Get(ydbschema.ChangefeedRef(table.Schema, table.Name, changefeed))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, placed(pending.source, "", fmt.Errorf("table %q declares no changefeed %q for consumer %q",
				table.Name, changefeed, pending.consumer.Name))
		}
		value, ok := object.Value.(*ydbschema.DesiredChangefeed)
		if !ok {
			return nil, fmt.Errorf("%w: changefeed consumer requires a desired changefeed", schemaext.ErrInvalidValue)
		}
		value.Spec.Consumers = append(value.Spec.Consumers, pending.consumer)
		if changefeeds, err = changefeeds.Replace(object); err != nil {
			return nil, err
		}
	}
	declared, err := changefeeds.All()
	if err != nil {
		return nil, err
	}
	contributions := make([]annotation.Contribution, 0, len(declared))
	for _, object := range declared {
		contributions = append(contributions, annotation.Contribution{Object: &object, Source: sources[object.Ref.String()],
			Label: fmt.Sprintf("changefeed %s", object.Ref)})
	}
	return contributions, nil
}

// finishTopics gives each topic of the file the consumers declared for it,
// then declares each topic. A consumer of a topic the file does not declare
// is refused rather than dropped, because a reader nobody creates is a
// declaration with no effect, and so is a second consumer of one name, which
// YDB refuses (`Consumer c defined more than once`). A topic declared twice
// is refused by its path.
func (d *fileDecoder) finishTopics(annotation.Tables) ([]annotation.Contribution, error) {
	for _, pending := range d.topicConsumers {
		index := slices.IndexFunc(d.topics, func(topic pendingTopic) bool {
			return topic.name == pending.topic && topic.schema == pending.schema
		})
		if index < 0 {
			return nil, placed(pending.source, ydbtopic.AttributeTopic, fmt.Errorf("the file declares no topic %q for consumer %q",
				ydbtopic.Display(pending.schema, pending.topic), pending.consumer.Name))
		}
		topic := &d.topics[index]
		if slices.ContainsFunc(topic.spec.Consumers, func(have ydbtopic.ConsumerSpec) bool { return have.Name == pending.consumer.Name }) {
			return nil, placed(pending.source, ydbtopic.AttributeTopic, fmt.Errorf("topic %q declares consumer %q twice",
				ydbtopic.Display(topic.schema, topic.name), pending.consumer.Name))
		}
		topic.spec.Consumers = append(topic.spec.Consumers, pending.consumer)
	}
	var contributions []annotation.Contribution
	for _, topic := range d.topics {
		next, err := ydbtopic.Declare(d.objects, topic.schema, topic.name, topic.source.Struct, topic.spec)
		if err != nil {
			return nil, placed(topic.source, "", refusal(err))
		}
		added, err := d.added(next, topic.source)
		if err != nil {
			return nil, err
		}
		contributions = append(contributions, added...)
	}
	return contributions, nil
}

// finishReplications gives each replication of the file the items declared
// for it, then declares each replication. An item of a replication the file
// does not declare is refused rather than dropped, because a table nobody
// replicates is a declaration with no effect, and so is a second item
// creating one replica path, where one table can stand. A replication left
// with no item is refused too: YDB takes none without a FOR clause.
func (d *fileDecoder) finishReplications(annotation.Tables) ([]annotation.Contribution, error) {
	for _, pending := range d.items {
		index := slices.IndexFunc(d.replications, func(replication pendingReplication) bool {
			return replication.name == pending.replication && replication.schema == pending.schema
		})
		if index < 0 {
			return nil, placed(pending.source, ydbreplication.AttributeReplication, fmt.Errorf(
				"the file declares no async replication %q for the item replicating %q",
				ydbreplication.Display(pending.schema, pending.replication), pending.item.Source))
		}
		replication := &d.replications[index]
		if slices.ContainsFunc(replication.spec.Items, func(have ydbreplication.Item) bool {
			return strings.Trim(have.Target, "/") == strings.Trim(pending.item.Target, "/")
		}) {
			return nil, placed(pending.source, ydbreplication.AttributeReplication, fmt.Errorf(
				"async replication %q declares two items with target %q",
				ydbreplication.Display(replication.schema, replication.name), pending.item.Target))
		}
		replication.spec.Items = append(replication.spec.Items, pending.item)
	}
	var contributions []annotation.Contribution
	for _, replication := range d.replications {
		if len(replication.spec.Items) == 0 {
			return nil, placed(replication.source, "", missingItems(fmt.Sprintf("async replication %q declares no item; declare the "+
				"tables it replicates with //%s in the same file",
				ydbreplication.Display(replication.schema, replication.name), directiveAsyncReplicationFor)))
		}
		next, err := ydbreplication.DeclareReplication(d.objects, replication.schema, replication.name,
			replication.source.Struct, replication.spec)
		if err != nil {
			return nil, placed(replication.source, "", refusal(err))
		}
		added, err := d.added(next, replication.source)
		if err != nil {
			return nil, err
		}
		contributions = append(contributions, added...)
	}
	return contributions, nil
}

// finishFamilies gives each table of the file the column families declared
// for it, as one facet. A family of a table the file does not declare is
// refused rather than dropped, and so are a second family of one name and a
// column two families name. Whether the columns a family names exist is the
// renderer's question, asked of the whole table.
func (d *fileDecoder) finishFamilies(tables annotation.Tables) ([]annotation.Contribution, error) {
	declared := make(map[int][]ydbschema.ColumnFamily)
	first := make(map[int]annotation.Declaration)
	var order []int
	for _, pending := range d.families {
		index, err := tables.Owning(pending.source.Struct, pending.source.Attributes[ydbfamily.AttributeTable], "a column family")
		if err != nil {
			return nil, placed(pending.source, "", err)
		}
		table := tables[index]
		if slices.ContainsFunc(declared[index], func(have ydbschema.ColumnFamily) bool { return have.Name == pending.spec.Name }) {
			return nil, placed(pending.source, "", fmt.Errorf("table %q declares column family %q twice", table.Name, pending.spec.Name))
		}
		if _, seen := declared[index]; !seen {
			order = append(order, index)
			first[index] = pending.source
		}
		declared[index] = append(declared[index], pending.spec)
		if err := ydbschema.ValidateDesiredColumnFamilies(&ydbschema.DesiredColumnFamilies{Families: declared[index]}); err != nil {
			if invalid, ok := errors.AsType[*schemaext.InvalidModelError](err); ok {
				reason := strings.TrimPrefix(invalid.Message, schemaext.ErrInvalidValue.Error()+": ")
				return nil, placed(pending.source, "", fmt.Errorf("table %q: %s", table.Name, reason))
			}
			return nil, err
		}
	}
	contributions := make([]annotation.Contribution, 0, len(order))
	for _, index := range order {
		source := first[index]
		contributions = append(contributions, annotation.Contribution{
			Facet:  &ydbschema.DesiredColumnFamilies{Families: declared[index]},
			Table:  source.Attributes[ydbfamily.AttributeTable],
			Label:  "column families",
			Source: source,
		})
	}
	return contributions, nil
}

func declareCoordinationNode(objects schemaext.Objects, kv map[string]string, holder string) (schemaext.Objects, error) {
	if err := ydbcoordination.RefuseName(kv["schema"], kv["name"]); err != nil {
		return objects, &annotation.DeclarationError{Attribute: "name", Err: err}
	}
	spec, err := ydbcoordination.ParseDeclaration(kv)
	if err != nil {
		return objects, err
	}
	return named(objects.With(ydbcoordination.DesiredObject(kv["schema"], kv["name"], holder, spec)))
}

func declarePool(objects schemaext.Objects, kv map[string]string, holder string) (schemaext.Objects, error) {
	name, spec, err := ydbworkload.ParsePool(kv)
	if err != nil {
		return objects, err
	}
	return named(objects.With(ydbworkload.DesiredPoolObject(name, holder, spec)))
}

func declareClassifier(objects schemaext.Objects, kv map[string]string, holder string) (schemaext.Objects, error) {
	name, spec, err := ydbworkload.ParseClassifier(kv)
	if err != nil {
		return objects, err
	}
	return named(objects.With(ydbworkload.DesiredClassifierObject(name, holder, spec)))
}

func declareSecret(objects schemaext.Objects, kv map[string]string, holder string) (schemaext.Objects, error) {
	valueEnv, err := ydbsecret.ParseValueEnv(kv)
	if err != nil {
		return objects, err
	}
	return ydbsecret.Declare(objects, kv[ydbsecret.AttributeSchema], kv[ydbsecret.AttributeName], holder, valueEnv)
}

func declareStreamingQuery(objects schemaext.Objects, kv map[string]string, holder string) (schemaext.Objects, error) {
	query := ydbstreaming.Desired{StructName: holder,
		Spec: ydbstreaming.Spec{Text: kv["text"], ResourcePool: kv["resource_pool"]}}
	if raw, exists := kv["run"]; exists {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return objects, &annotation.DeclarationError{Attribute: "run", Err: fmt.Errorf("streaming query %q run: %w", kv["name"], err)}
		}
		query.Spec.Run = new(value)
	}
	if raw, exists := kv["allow_state_reset"]; exists {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return objects, &annotation.DeclarationError{Attribute: "allow_state_reset",
				Err: fmt.Errorf("streaming query %q allow_state_reset: %w", kv["name"], err)}
		}
		query.AllowStateReset = value
	}
	if err := ydbstreaming.Validate(query.Spec); err != nil {
		return objects, fmt.Errorf("streaming query %q: %w", kv["name"], err)
	}
	ref := ydbstreaming.Ref(kv["schema"], kv["name"])
	if err := ydbstreaming.ValidateIdentity(ref); err != nil {
		return objects, &annotation.DeclarationError{Attribute: "name", Err: err}
	}
	return named(objects.With(schemaext.Object{Ref: ref, Value: &query}))
}

func declareExternalDataSource(objects schemaext.Objects, kv map[string]string, holder string) (schemaext.Objects, error) {
	options, err := ydbexternal.ParseOptions(kv[ydbexternal.AttributeOptions], ydbexternal.DataSourceReserved...)
	if err != nil {
		return objects, err
	}
	return ydbexternal.DeclareSource(objects, kv[ydbexternal.AttributeSchema], kv[ydbexternal.AttributeName], holder,
		ydbexternal.DataSource{
			SourceType: strings.TrimSpace(kv[ydbexternal.AttributeSourceType]),
			Location:   strings.TrimSpace(kv[ydbexternal.AttributeLocation]),
			AuthMethod: strings.TrimSpace(kv[ydbexternal.AttributeAuthMethod]),
			Options:    options,
		})
}

func declareExternalTable(objects schemaext.Objects, kv map[string]string, holder string) (schemaext.Objects, error) {
	columns, err := ydbexternal.ParseColumns(kv[ydbexternal.AttributeColumns])
	if err != nil {
		return objects, err
	}
	options, err := ydbexternal.ParseOptions(kv[ydbexternal.AttributeOptions], ydbexternal.TableReserved...)
	if err != nil {
		return objects, err
	}
	return ydbexternal.DeclareTable(objects, kv[ydbexternal.AttributeSchema], kv[ydbexternal.AttributeName], holder,
		ydbexternal.Table{
			DataSource: strings.TrimSpace(kv[ydbexternal.AttributeDataSource]),
			Location:   strings.TrimSpace(kv[ydbexternal.AttributeLocation]),
			Columns:    columns,
			Options:    options,
		})
}

func declareTransfer(objects schemaext.Objects, kv map[string]string, holder string) (schemaext.Objects, error) {
	spec, err := ydbreplication.ParseTransfer(kv)
	if err != nil {
		return objects, err
	}
	return ydbreplication.DeclareTransfer(objects, kv[ydbreplication.AttributeSchema], kv[ydbreplication.AttributeName], holder, spec)
}

// named attributes a second declaration of an object to its name.
func named(objects schemaext.Objects, err error) (schemaext.Objects, error) {
	if errors.Is(err, schemaext.ErrDuplicate) {
		return objects, &annotation.DeclarationError{Attribute: "name", Err: err}
	}
	return objects, err
}

// placed names the declaration a refusal at the end of the file is about,
// and the attribute where attribute is not empty. It keeps the attribute a
// refusal already names.
func placed(source annotation.Declaration, attribute string, err error) error {
	if declared, ok := errors.AsType[*annotation.DeclarationError](err); ok {
		attribute = declared.Attribute
		err = declared.Err
	}
	return &annotation.DeclarationError{Declaration: source, Attribute: attribute, Err: err}
}

// refusal names the attribute a YDB grammar's refusal is about.
func refusal(err error) error {
	if declared, ok := errors.AsType[*annotation.DeclarationError](err); ok {
		return declared
	}
	for _, attributeOf := range refusedAttributes {
		if attribute, ok := attributeOf(err); ok {
			return &annotation.DeclarationError{Attribute: attribute, Err: err}
		}
	}
	return &annotation.DeclarationError{Err: err}
}

// refusedAttributes read the attribute out of each refusal a YDB grammar
// returns. A second declaration of an object is about its name.
var refusedAttributes = []func(error) (string, bool){
	attributeOf(func(e *ydbchangefeed.DeclarationError) string { return e.Attribute }),
	attributeOf(func(e *ydbtopic.DeclarationError) string { return e.Attribute }),
	attributeOf(func(e *ydbfamily.DeclarationError) string { return e.Attribute }),
	attributeOf(func(e *ydbsecret.DeclarationError) string { return e.Attribute }),
	attributeOf(func(e *ydbworkload.DeclarationError) string { return e.Attribute }),
	attributeOf(func(e *ydbexternal.DeclarationError) string { return e.Attribute }),
	attributeOf(func(e *ydbreplication.DeclarationError) string { return e.Attribute }),
	attributeOf(func(e *ydbcoordination.SettingError) string { return e.Setting }),
	attributeOf(func(e *ydbpartition.DeclarationError) string { return e.Attribute }),
	attributeOf(func(*ydbsecret.DuplicateError) string { return ydbsecret.AttributeName }),
	attributeOf(func(*ydbtopic.DuplicateError) string { return ydbtopic.AttributeName }),
	attributeOf(func(*ydbexternal.DuplicateError) string { return ydbexternal.AttributeName }),
	attributeOf(func(*ydbreplication.DuplicateError) string { return ydbreplication.AttributeName }),
}

func attributeOf[E error](name func(E) string) func(error) (string, bool) {
	return func(err error) (string, bool) {
		target, ok := errors.AsType[E](err)
		if !ok {
			return "", false
		}
		return name(target), true
	}
}

// missingItems refuses a replication without items. It reads as its message
// and matches [ptaherr.ErrMissingRequiredAttribute].
type missingItems string

func (e missingItems) Error() string { return string(e) }

// Unwrap returns [ptaherr.ErrMissingRequiredAttribute].
func (missingItems) Unwrap() error { return ptaherr.ErrMissingRequiredAttribute }

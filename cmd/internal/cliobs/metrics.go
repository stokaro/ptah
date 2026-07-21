package cliobs

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stokaro/ptah/migration/migrator"
)

var durationBuckets = []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60}

type metricsServer struct {
	server *http.Server
	data   *metricsData
}

func startMetricsServer(addr string) (*metricsServer, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, nil
	}
	data := &metricsData{
		counters:   make(map[string]*counterSeries),
		histograms: make(map[string]*histogramSeries),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", data.ServeHTTP)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("start metrics listener %s: %w", addr, err)
	}
	go func() {
		_ = server.Serve(listener)
	}()
	return &metricsServer{server: server, data: data}, nil
}

func (s *metricsServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *metricsServer) StartSpan(ctx context.Context, _ string, _ ...migrator.ObservationAttribute) (context.Context, migrator.ObservationSpan) {
	_, span := migrator.NoopObserver{}.StartSpan(ctx, "")
	return ctx, span
}

func (s *metricsServer) AddCounter(_ context.Context, name string, value int64, attrs ...migrator.ObservationAttribute) {
	s.data.addCounter(name, value, attrs)
}

func (s *metricsServer) RecordDuration(_ context.Context, name string, duration time.Duration, attrs ...migrator.ObservationAttribute) {
	s.data.recordDuration(name, duration.Seconds(), attrs)
}

type metricsData struct {
	mu         sync.Mutex
	counters   map[string]*counterSeries
	histograms map[string]*histogramSeries
}

func (d *metricsData) addCounter(name string, value int64, attrs []migrator.ObservationAttribute) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := newSeriesKey(name, attrs)
	series := d.counters[key.id()]
	if series == nil {
		series = &counterSeries{key: key}
		d.counters[key.id()] = series
	}
	series.value += value
}

func (d *metricsData) recordDuration(name string, value float64, attrs []migrator.ObservationAttribute) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := newSeriesKey(name, attrs)
	series := d.histograms[key.id()]
	if series == nil {
		series = &histogramSeries{key: key, buckets: make([]uint64, len(durationBuckets))}
		d.histograms[key.id()] = series
	}
	for i, bucket := range durationBuckets {
		if value <= bucket {
			series.buckets[i]++
		}
	}
	series.count++
	series.sum += value
}

func (d *metricsData) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	counterKeys := sortedCounterKeys(d.counters)
	writeCounterSeries(w, counterKeys)

	histogramKeys := sortedHistogramKeys(d.histograms)
	writeHistogramSeries(w, histogramKeys)
}

func writeCounterSeries(w io.Writer, series []*counterSeries) {
	var currentName string
	for _, item := range series {
		if item.key.name != currentName {
			fmt.Fprintf(w, "# TYPE %s counter\n", item.key.name)
			currentName = item.key.name
		}
		fmt.Fprintf(w, "%s%s %d\n", item.key.name, item.key.promLabels(""), item.value)
	}
}

func writeHistogramSeries(w io.Writer, series []*histogramSeries) {
	var currentName string
	for _, item := range series {
		if item.key.name != currentName {
			fmt.Fprintf(w, "# TYPE %s histogram\n", item.key.name)
			currentName = item.key.name
		}
		for i, bucket := range durationBuckets {
			fmt.Fprintf(w, "%s_bucket%s %d\n", item.key.name, item.key.promLabels(fmt.Sprintf(`le="%.3g"`, bucket)), item.buckets[i])
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", item.key.name, item.key.promLabels(`le="+Inf"`), item.count)
		fmt.Fprintf(w, "%s_sum%s %.9g\n", item.key.name, item.key.promLabels(""), item.sum)
		fmt.Fprintf(w, "%s_count%s %d\n", item.key.name, item.key.promLabels(""), item.count)
	}
}

type counterSeries struct {
	key   seriesKey
	value int64
}

type histogramSeries struct {
	key     seriesKey
	buckets []uint64
	count   uint64
	sum     float64
}

type seriesKey struct {
	name   string
	labels []label
}

type label struct {
	key   string
	value string
}

func newSeriesKey(name string, attrs []migrator.ObservationAttribute) seriesKey {
	labels := make([]label, 0, len(attrs))
	for _, attr := range attrs {
		if attr.Key == "" || attr.Value == nil {
			continue
		}
		labels = append(labels, label{key: sanitizeLabelName(attr.Key), value: fmt.Sprint(attr.Value)})
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].key == labels[j].key {
			return labels[i].value < labels[j].value
		}
		return labels[i].key < labels[j].key
	})
	return seriesKey{name: sanitizeMetricName(name), labels: labels}
}

func (k seriesKey) promLabels(extra string) string {
	parts := make([]string, 0, len(k.labels)+1)
	if extra != "" {
		parts = append(parts, extra)
	}
	for _, label := range k.labels {
		parts = append(parts, fmt.Sprintf(`%s=%q`, label.key, label.value))
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (k seriesKey) id() string {
	return k.name + k.promLabels("")
}

func sortedCounterKeys(values map[string]*counterSeries) []*counterSeries {
	keys := make([]*counterSeries, 0, len(values))
	for _, series := range values {
		keys = append(keys, series)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].key.id() < keys[j].key.id() })
	return keys
}

func sortedHistogramKeys(values map[string]*histogramSeries) []*histogramSeries {
	keys := make([]*histogramSeries, 0, len(values))
	for _, series := range values {
		keys = append(keys, series)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].key.id() < keys[j].key.id() })
	return keys
}

func sanitizeMetricName(name string) string {
	return sanitizePromName(name)
}

func sanitizeLabelName(name string) string {
	return sanitizePromName(strings.ReplaceAll(name, ".", "_"))
}

func sanitizePromName(name string) string {
	var b strings.Builder
	for i, r := range name {
		ok := r == '_' || r == ':' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9'
		if ok {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	if b.Len() == 0 {
		return "ptah_metric"
	}
	return b.String()
}

// Tests for the metrics: every metric exists, and none is labeled with a
// batch or request ID.
package metrics_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/ananya-mh/batch-inference/internal/metrics"
	"github.com/ananya-mh/batch-inference/internal/testdb"
	"github.com/ananya-mh/batch-inference/internal/testfixtures"
)

// allowedLabels are the only label names our metrics may use.
var allowedLabels = map[string]bool{"tenant": true, "outcome": true}

// idLike matches values that look like a batch, file, or request ID.
var idLike = regexp.MustCompile(`^(batch_|file-|batch_req_|chatcmpl-)|-[0-9]+$`)

// exerciseAll records one value in every metric, so each labeled metric has
// at least one series to inspect.
func exerciseAll(m *metrics.Metrics) {
	m.WorkersBusy.Set(1)
	m.WorkersTotal.Set(16)
	for _, outcome := range []string{metrics.OutcomeCompleted, metrics.OutcomeFailed, metrics.OutcomeRetried, metrics.OutcomeExpired, metrics.OutcomeCancelled} {
		m.RequestOutcomes.WithLabelValues("tenant_a", outcome).Inc()
	}
	m.RequestWaitSeconds.WithLabelValues("tenant_a").Observe(1.5)
	m.ModelCallSeconds.WithLabelValues(metrics.CallSucceeded).Observe(0.2)
	m.SchedulerPassSeconds.WithLabelValues("ran").Observe(0.004)
}

// ourFamilies gathers the metrics and returns ours (named batch_...), by name.
func ourFamilies(t *testing.T, m *metrics.Metrics) map[string]*dto.MetricFamily {
	t.Helper()
	gathered, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	families := map[string]*dto.MetricFamily{}
	for _, family := range gathered {
		if strings.HasPrefix(family.GetName(), "batch_") {
			families[family.GetName()] = family
		}
	}
	return families
}

// labelProblems returns one line for each label that is not tenant or
// outcome, or whose value looks like an ID.
func labelProblems(families map[string]*dto.MetricFamily) []string {
	var problems []string
	for name, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if !allowedLabels[label.GetName()] {
					problems = append(problems, fmt.Sprintf("%s has label %q; only tenant and outcome are allowed", name, label.GetName()))
				}
				if idLike.MatchString(label.GetValue()) {
					problems = append(problems, fmt.Sprintf("%s has label %s=%q, which looks like an ID", name, label.GetName(), label.GetValue()))
				}
			}
		}
	}
	return problems
}

// TestNoMetricIsLabeledByID checks every metric, including the per-tenant
// ones read from the database, only uses the labels tenant and outcome, and
// never carries a batch or request ID.
func TestNoMetricIsLabeledByID(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	// A batch with requests in the database, so the database metrics have
	// series too.
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 3)
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO requests (batch_id, custom_id, tenant_id, line_number) VALUES ($1, 'a-0', 'tenant_a', 1)`, batch.ID); err != nil {
		t.Fatalf("insert request: %v", err)
	}
	m := metrics.New(pool)
	exerciseAll(m)

	families := ourFamilies(t, m)
	want := []string{
		"batch_workers_busy", "batch_workers_total", "batch_request_outcomes_total",
		"batch_request_wait_seconds", "batch_model_call_seconds", "batch_scheduler_pass_seconds",
		"batch_queue_depth", "batch_requests_in_flight", "batch_requests_waiting_for_retry",
	}
	for _, name := range want {
		if families[name] == nil {
			t.Errorf("metric %s is missing", name)
		}
	}
	for _, problem := range labelProblems(families) {
		t.Error(problem)
	}
}

// TestLabelCheckCatchesIDs makes sure the label check itself works: a metric
// labeled with a batch ID must be caught.
func TestLabelCheckCatchesIDs(t *testing.T) {
	t.Parallel()
	m := metrics.New(nil)
	// Misuse on purpose: a batch ID where a tenant belongs.
	m.RequestWaitSeconds.WithLabelValues("batch_k3j2h4g5").Observe(1)

	if problems := labelProblems(ourFamilies(t, m)); len(problems) == 0 {
		t.Error("a batch ID used as a label value was not caught")
	}
}

// TestDatabaseMetricsCountPerTenant checks the per-tenant numbers read from
// the database at scrape time.
func TestDatabaseMetricsCountPerTenant(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 3)
	if _, err := pool.Exec(ctx, `
		INSERT INTO requests (batch_id, custom_id, tenant_id, line_number, status) VALUES
			($1, 'a-0', 'tenant_a', 1, 'pending'),
			($1, 'a-1', 'tenant_a', 2, 'pending'),
			($1, 'a-2', 'tenant_a', 3, 'queued')`, batch.ID); err != nil {
		t.Fatalf("insert requests: %v", err)
	}
	families := ourFamilies(t, metrics.New(pool))

	value := func(name string) float64 {
		family := families[name]
		if family == nil || len(family.GetMetric()) != 1 {
			t.Fatalf("%s: want one series, got %v", name, family)
		}
		return family.GetMetric()[0].GetGauge().GetValue()
	}
	if got := value("batch_queue_depth"); got != 2 {
		t.Errorf("queue depth = %v, want 2", got)
	}
	if got := value("batch_requests_in_flight"); got != 1 {
		t.Errorf("in flight = %v, want 1", got)
	}
}

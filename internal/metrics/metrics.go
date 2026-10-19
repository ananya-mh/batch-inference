// Package metrics defines the service's Prometheus metrics and serves them.
//
// Labels are only ever "tenant" and "outcome", never a batch or request ID:
// IDs would create a new time series for every batch and request, which
// Prometheus is not built to store. A test checks this.
package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Request outcomes, the values of the "outcome" label on RequestOutcomes.
const (
	OutcomeCompleted = "completed" // saved a successful result
	OutcomeFailed    = "failed"    // saved a failed result
	OutcomeRetried   = "retried"   // an attempt failed and will be tried again
	OutcomeExpired   = "expired"   // the batch's deadline passed first
	OutcomeCancelled = "cancelled" // the batch was cancelled first
)

// Model call outcomes, the values of the "outcome" label on ModelCallSeconds.
const (
	CallSucceeded = "success"
	CallFailed    = "error"
)

// collectTimeout limits the database queries run when Prometheus scrapes.
const collectTimeout = 5 * time.Second

// Metrics holds every metric. Each server (and each test) makes its own with
// New, so nothing is shared between them.
type Metrics struct {
	registry *prometheus.Registry

	// WorkersBusy is how many of this server's workers are running a job.
	WorkersBusy prometheus.Gauge
	// WorkersTotal is how many workers this server has.
	WorkersTotal prometheus.Gauge
	// RequestOutcomes counts what happened to requests, by tenant and outcome.
	RequestOutcomes *prometheus.CounterVec
	// RequestWaitSeconds is, per tenant, how long a request waited from its
	// batch's submission until its model call first started.
	RequestWaitSeconds *prometheus.HistogramVec
	// ModelCallSeconds is how long model calls took, by outcome.
	ModelCallSeconds *prometheus.HistogramVec
	// SchedulerPassSeconds is how long scheduler passes took. The outcome is
	// "ran", or "skipped" when another server held the scheduler lock.
	SchedulerPassSeconds *prometheus.HistogramVec
}

// New creates the metrics. If pool is not nil, scrapes also report the
// per-tenant numbers that live in the database (queue depth, requests in
// flight, requests waiting for a retry).
func New(pool *pgxpool.Pool) *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		WorkersBusy: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "batch_workers_busy",
			Help: "Workers on this server currently running a request.",
		}),
		WorkersTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "batch_workers_total",
			Help: "Workers on this server.",
		}),
		RequestOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "batch_request_outcomes_total",
			Help: "Requests by what happened to them: completed, failed, retried (one per retried attempt), expired, cancelled.",
		}, []string{"tenant", "outcome"}),
		RequestWaitSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_request_wait_seconds",
			Help:    "Time from a batch's submission until a request's model call first started.",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		}, []string{"tenant"}),
		ModelCallSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_model_call_seconds",
			Help:    "Duration of calls to the model server.",
			Buckets: []float64{0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
		}, []string{"outcome"}),
		SchedulerPassSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_scheduler_pass_seconds",
			Help:    "Duration of scheduler passes; outcome is ran, or skipped when another server held the lock.",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"outcome"}),
	}

	m.registry.MustRegister(
		m.WorkersBusy, m.WorkersTotal, m.RequestOutcomes, m.RequestWaitSeconds,
		m.ModelCallSeconds, m.SchedulerPassSeconds,
		// Standard Go runtime and process metrics (memory, goroutines, CPU).
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	if pool != nil {
		m.registry.MustRegister(newDatabaseCollector(pool))
	}
	return m
}

// Handler serves the metrics in Prometheus's text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry returns the registry, for tests that inspect the metrics.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// Tally adds up request outcomes inside a transaction, so they can be
// recorded after it commits. Counting before the commit would count work
// that a rollback then undoes.
type Tally map[[2]string]int

// Add counts n requests of tenant with outcome.
func (t Tally) Add(tenant, outcome string, n int) {
	if n > 0 {
		t[[2]string{tenant, outcome}] += n
	}
}

// Record adds a committed tally to RequestOutcomes.
func (m *Metrics) Record(tally Tally) {
	for key, n := range tally {
		m.RequestOutcomes.WithLabelValues(key[0], key[1]).Add(float64(n))
	}
}

// databaseCollector reports per-tenant numbers that live in the database,
// read fresh at every scrape. They describe the whole service, not just this
// server, so with several servers use max() rather than sum() across them.
type databaseCollector struct {
	pool         *pgxpool.Pool
	queueDepth   *prometheus.Desc
	inFlight     *prometheus.Desc
	waitingRetry *prometheus.Desc
}

// newDatabaseCollector returns a collector that queries pool.
func newDatabaseCollector(pool *pgxpool.Pool) *databaseCollector {
	return &databaseCollector{
		pool: pool,
		queueDepth: prometheus.NewDesc("batch_queue_depth",
			"Requests waiting in the requests table for a slot in the window (the queue the fair scheduler picks from), per tenant.",
			[]string{"tenant"}, nil),
		inFlight: prometheus.NewDesc("batch_requests_in_flight",
			"Requests holding a slot in the window (queued in River or running, including waiting for a retry), per tenant.",
			[]string{"tenant"}, nil),
		waitingRetry: prometheus.NewDesc("batch_requests_waiting_for_retry",
			"In-flight requests whose last attempt failed and that are waiting out the retry delay, per tenant.",
			[]string{"tenant"}, nil),
	}
}

// Describe sends the descriptions of the collector's metrics.
func (c *databaseCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- c.queueDepth
	descriptions <- c.inFlight
	descriptions <- c.waitingRetry
}

// Collect queries the database and sends the current values. Tenants with
// no unfinished requests are left out. If the query fails, it logs the
// error and sends nothing, so one failed query does not break the scrape.
func (c *databaseCollector) Collect(values chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), collectTimeout)
	defer cancel()

	rows, err := c.pool.Query(ctx, `
		SELECT r.tenant_id,
		       count(*) FILTER (WHERE r.status = 'pending'),
		       count(*) FILTER (WHERE r.status IN ('queued', 'running')),
		       count(*) FILTER (WHERE j.state IN ('retryable', 'scheduled'))
		FROM requests r
		LEFT JOIN river_job j ON j.id = r.river_job_id AND r.status = 'running'
		WHERE r.status IN ('pending', 'queued', 'running')
		GROUP BY r.tenant_id`)
	if err != nil {
		slog.Error("metrics: query per-tenant request counts", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tenant string
		var queued, inFlight, waitingRetry float64
		if err := rows.Scan(&tenant, &queued, &inFlight, &waitingRetry); err != nil {
			slog.Error("metrics: read per-tenant request counts", "error", err)
			return
		}
		values <- prometheus.MustNewConstMetric(c.queueDepth, prometheus.GaugeValue, queued, tenant)
		values <- prometheus.MustNewConstMetric(c.inFlight, prometheus.GaugeValue, inFlight, tenant)
		values <- prometheus.MustNewConstMetric(c.waitingRetry, prometheus.GaugeValue, waitingRetry, tenant)
	}
	if err := rows.Err(); err != nil {
		slog.Error("metrics: read per-tenant request counts", "error", err)
	}
}

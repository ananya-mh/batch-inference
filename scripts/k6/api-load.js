// k6 load test of the API layer: many clients polling batch status and
// listing batches while batches run. Started by scripts/load-test.sh, which
// passes BASE_URL, TENANTS (JSON: [{"key": ..., "batch": ...}]), VUS,
// DURATION and OUT.
import http from "k6/http";
import { check, sleep } from "k6";

const tenants = JSON.parse(__ENV.TENANTS || "[]");

export const options = {
	vus: Number(__ENV.VUS || 50),
	duration: __ENV.DURATION || "60s",
	summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max", "count"],
	// Thresholds on each endpoint's tag make k6 report each endpoint
	// separately; they are set loose on purpose and only fail on errors.
	thresholds: {
		"http_req_duration{endpoint:status}": ["p(99)<10000"],
		"http_req_duration{endpoint:list}": ["p(99)<10000"],
		"http_req_failed{endpoint:status}": ["rate<0.01"],
		"http_req_failed{endpoint:list}": ["rate<0.01"],
	},
};

// One client's round: check a batch's status and list the tenant's batches,
// like an SDK polling for results, then wait a little before the next round.
export default function () {
	const tenant = tenants[Math.floor(Math.random() * tenants.length)];
	const headers = { Authorization: `Bearer ${tenant.key}` };

	const status = http.get(`${__ENV.BASE_URL}/v1/batches/${tenant.batch}`, {
		headers,
		tags: { endpoint: "status" },
	});
	check(status, { "status 200": (r) => r.status === 200 });

	const list = http.get(`${__ENV.BASE_URL}/v1/batches?limit=20`, {
		headers,
		tags: { endpoint: "list" },
	});
	check(list, { "list 200": (r) => r.status === 200 });

	sleep(0.1 + Math.random() * 0.4);
}

// Writes the per-endpoint results as a Markdown table, plus k6's full JSON.
export function handleSummary(data) {
	const seconds = data.state.testRunDurationMs / 1000;
	const row = (label, tag) => {
		const d = data.metrics[`http_req_duration{endpoint:${tag}}`].values;
		const failed = data.metrics[`http_req_failed{endpoint:${tag}}`].values;
		const ms = (v) => v.toFixed(1);
		return `| ${label} | ${d.count} | ${(d.count / seconds).toFixed(0)} | ${ms(d["p(50)"])} | ${ms(d["p(95)"])} | ${ms(d["p(99)"])} | ${ms(d.max)} | ${(failed.rate * 100).toFixed(2)}% |`;
	};
	const table = [
		"| Endpoint | Requests | Requests/s | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Errors |",
		"|---|---|---|---|---|---|---|---|",
		row("`GET /v1/batches/{id}` (status)", "status"),
		row("`GET /v1/batches?limit=20` (list)", "list"),
	].join("\n");
	return {
		[`${__ENV.OUT}/k6-table.md`]: table + "\n",
		[`${__ENV.OUT}/k6-summary.json`]: JSON.stringify(data, null, 2),
		stdout: table + "\n",
	};
}

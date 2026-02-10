# Design Spec: `qontrol status` — Multi-Cluster Dashboard

**Author:** Mayor (Gas Town)
**Date:** 2026-02-10
**Status:** Approved
**Stakeholder:** Daniel Motles (Overseer)

## 1. Overview

Replace the existing single-cluster `dashboard` command with a comprehensive
multi-cluster `status` command. This is a one-shot CLI report that shows
environment-wide health, capacity, connections, and performance across all
configured clusters. It supports `--watch` mode for live refresh and `--json`
for programmatic consumption.

### Goals

- Single command to see the health of an entire multi-cluster environment
- Alerts bubble to the top — actionable items first
- Graceful degradation when clusters are unreachable (cached stale data)
- Works with any mix of on-prem hardware, CNQ (AWS), and ANQ (Azure) clusters
- Naturally degrades to single-cluster view when only one is configured
- Dense, scannable, well-styled terminal output

### Audience

Primary: Customers running Qumulo clusters
Secondary: Field Engineers, SEs, Customer Success

## 2. Command UX

### Invocation

```
qontrol status [flags]
```

Replaces the existing `qontrol dashboard` command. `dashboard` becomes an
alias for `status` for backward compatibility.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--watch` | off | Continuous refresh mode |
| `--interval <secs>` | 2 | Refresh interval in watch mode |
| `--json` | off | Output structured JSON instead of formatted text |
| `--profile <name>` | all | Limit to specific profile(s). Can be repeated. When omitted, shows ALL configured profiles. |
| `--timeout <secs>` | 30 | Per-cluster API timeout |
| `--no-cache` | off | Skip reading/writing cache (always fetch fresh) |
| `--quiet` | off | Suppress non-essential output |
| `-v` | off | Verbose/debug output |

### Behavior

1. Load config, enumerate all profiles (or filter by `--profile`)
2. Fan out API calls to all clusters in parallel (one thread per cluster)
3. For each cluster: collect all data, measure API response latency
4. If a cluster is unreachable: load last-good data from cache, mark stale
5. Write successful results to cache
6. Render output (or emit JSON)
7. In `--watch` mode: clear screen, repeat from step 2

## 3. Output Format

### 3.1 Overview Section

```
═══ Environment Overview ═══════════════════════════════════════════════════════
  Clusters: 5 (4 healthy, 1 unreachable)    Latency: 8-142ms
  Nodes:    22 total (21 online, 1 offline)
  Capacity: 1.86 PB / 2.17 PB (85.6%)
  Files:    698,412,061    Dirs: 48,231,004    Snapshots: 12,847 (7.7 TB)
```

Aggregates across ALL clusters (including stale data from unreachable ones).
Latency range shows min-max API response time across reachable clusters.

### 3.2 Alerts Section

```
═══ Alerts ═════════════════════════════════════════════════════════════════════
  ✗ iss-sg node 4: OFFLINE
  ⚠ gravytrain-sg: projected to fill in ~62 days (+1.2 TB/day)
  ⚠ iss-sg: projected to fill in ~78 days (+0.8 TB/day)
  ⚠ music: 1 disk unhealthy (node 3, bay 12, HDD)
  ✗ az-dev: UNREACHABLE (last seen 2h ago)
```

Alerts are sorted by severity: `✗` (critical) before `⚠` (warning).

**Alert sources:**

| Condition | Severity | Message |
|-----------|----------|---------|
| Cluster unreachable | ✗ critical | `UNREACHABLE (last seen <time>)` |
| Node offline | ✗ critical | `<cluster> node <N>: OFFLINE` |
| Disk unhealthy | ⚠ warning | `<cluster>: N disk(s) unhealthy (node N, bay N, type)` |
| PSU unhealthy | ⚠ warning | `<cluster>: PSU issue (node N, <location>)` |
| Data at risk (restriper) | ✗ critical | `<cluster>: DATA AT RISK — restriper active` |
| Protection degraded | ⚠ warning | `<cluster>: fault tolerance degraded (N node failures remaining)` |
| Capacity fill <90d (on-prem) | ⚠ warning | `projected to fill in ~Nd (+X TB/day)` |
| Capacity fill <7d (cloud) | ⚠ warning | `may run out of space soon — consider increasing capacity clamp` |

If no alerts: display `  No issues detected.`

### 3.3 Per-Cluster Sections

#### Healthy on-prem cluster:

```
  gravytrain-sg                    on-prem · C192T, QCT_D52T · v7.8.0 · 42ms
  ────────────────────────────────────────────────────────────────────────────
  Nodes:    5/5 online
  Capacity: 594 TB / 605 TB (98.2%) ████████████████████░  snaps: 6.7 TB
  Files:    501,204,881    Dirs: 32,401,221    Snapshots: 8,201
  Activity: R: 140 IOPS / 57.8 MB/s    W: 122 IOPS / 1.6 MB/s
  ⚠ Projected to fill in ~62 days (30d avg growth: +1.2 TB/day)

  Connections            NIC Throughput
  node1:  14  ████████   node1: 12.4 / 200 Gbps ▸▸▸░░░░░░░  6%
  node2:   3  ██         node2:  1.1 / 200 Gbps ▸░░░░░░░░░  1%
  node3:   3  ██         node3:  0.8 / 200 Gbps ▸░░░░░░░░░  <1%
  node4:   0             node4:  0.2 / 100 Gbps ░░░░░░░░░░  <1%
  node5:   1  █          node5:  0.4 / 100 Gbps ░░░░░░░░░░  <1%
```

#### Healthy cloud cluster (CNQ/ANQ):

```
  aws-gravytrain                   CNQ · AWS · v7.8.0 · 142ms
  ────────────────────────────────────────────────────────────────────────────
  Nodes:    3/3 online
  Capacity: 77.3 TB / 454.7 TB (17.0%) ████░░░░░░░░░░░░░░░░░  snaps: 0 B
  Files:    35,679    Dirs: 1,452    Snapshots: 0
  Activity: idle

  Connections            NIC Throughput
  node1:   0             node1:  0.0 Gbps
  node2:   1  █          node2:  0.0 Gbps
  node3:   0             node3:  0.0 Gbps
```

Cloud clusters:
- No hardware SKU in header (show cloud provider instead)
- No "of X Gbps link" on NIC — cloud link speeds are fluid/opaque
- Capacity projection uses shorter warning threshold (7 days, not 90)
- Capacity warning message suggests increasing capacity clamp, not buying hardware

#### Cluster with offline node:

```
  iss-sg                           on-prem · C192T, QCT_D52T · v7.8.0 · 38ms
  ────────────────────────────────────────────────────────────────────────────
  Nodes:    5/6 online (node 4: OFFLINE)
  Capacity: 707 TB / 736 TB (96.1%) ███████████████████░░  snaps: 980 GB
  ...
  node4:   —  OFFLINE    node4:    — OFFLINE
```

#### Unreachable cluster (stale cached data):

```
  az-dev                           ANQ · Azure · v7.8.0
  ────────────────────────────────────────────────────────────────────────────
  ✗ UNREACHABLE — last seen 2026-02-10 09:41 (2h ago)
  Capacity: 22.1 TB / 454.7 TB (4.9%) █░░░░░░░░░░░░░░░░░░░░  snaps: 0 B
  Files:    1,201    Dirs: 84    Snapshots: 0
  (stale data from last successful poll)
```

No latency in header (can't measure). No activity data (stale). Capacity
and file counts from cache.

### 3.4 Idle vs Active Activity

When a cluster has zero IOPS and zero throughput, show `Activity: idle`
instead of showing zeros.

## 4. Cluster Type Detection

Use the `model_number` field from `GET /v1/cluster/nodes/`:

| model_number value | Cluster type | Display |
|-------------------|-------------|---------|
| `"AWS"` | Cloud Native Qumulo on AWS | `CNQ · AWS` |
| `"Azure"` | Azure Native Qumulo | `ANQ · Azure` |
| Any other value (C192T, Q0626, QCT_*, etc.) | On-premises hardware | `on-prem · <unique SKUs>` |

For on-prem, collect unique `model_number` values across all nodes in the
cluster and display as comma-separated list (e.g., `C192T, QCT_D52T`).

When the metrics API is available (`PRIVILEGE_METRICS_READ`), optionally
enrich with `qumulo_info{platform=..., service_model=...}` labels for
higher confidence detection.

## 5. Capacity Projection Algorithm

### Data Source

`GET /v1/analytics/capacity-history/?begin-time={epoch}&interval=DAILY`

Fetch last 30 days of daily capacity data points.

### Algorithm

1. Collect daily `capacity_used` values for the last 30 days
2. Compute linear regression (least squares) on used capacity over time
3. Slope = average daily growth rate (TB/day)
4. If slope <= 0: no growth, no warning (cluster is stable or shrinking)
5. If slope > 0: extrapolate to `total_usable` capacity
   - `days_to_full = (total_usable - current_used) / slope`
6. Apply threshold based on cluster type:
   - **On-prem**: warn if `days_to_full < 90`
   - **Cloud (CNQ/ANQ)**: warn if `days_to_full < 7`

### Display

On-prem warning:
```
⚠ Projected to fill in ~62 days (30d avg growth: +1.2 TB/day)
```

Cloud warning:
```
⚠ May run out of space within ~5 days — consider increasing capacity clamp
```

### Edge Cases

- Less than 7 days of history: skip projection, don't warn
- Highly variable growth (R-squared < 0.5): consider noting "unstable growth pattern"
- Cluster at >95% but zero growth: no warning (they're maintaining, not growing)

## 6. Data Sources — API Endpoint Mapping

### Per-cluster API calls (all called in parallel per cluster):

| Data needed | Endpoint | Notes |
|-------------|----------|-------|
| Cluster name | `GET /v1/cluster/settings` | `cluster_name` field |
| Software version | `GET /v1/version` | `revision_id` field |
| Cluster UUID | `GET /v1/node/state` | `cluster_id` field |
| Node list + status | `GET /v1/cluster/nodes/` | `node_status`, `model_number`, array length = node count |
| Disk health | `GET /v1/cluster/slots/` | `state` field, flag if not `"healthy"` |
| PSU health | `GET /v1/cluster/nodes/chassis/` | `psu_statuses[].state`, flag if not `"GOOD"`. Cloud returns empty array (expected). |
| Protection status | `GET /v1/cluster/protection/status` | `remaining_node_failures`, `remaining_drive_failures` |
| Restriper / data-at-risk | `GET /v1/cluster/restriper/status` | `data_at_risk` bool, `status` field |
| Capacity | `GET /v1/file-system` | `total_size_bytes`, `free_size_bytes`, `snapshot_size_bytes` |
| Capacity history (30d) | `GET /v1/analytics/capacity-history/?begin-time={30d-ago}&interval=DAILY` | For projection algorithm |
| Active connections | `GET /v2/network/connections/` | Per-node, per-protocol connection list |
| NIC stats + link speed | `GET /v3/network/status` | Per-node: `bytes_sent`, `bytes_received`, `speed` per device. Also has `cloud_status` for cloud detection. |
| File/dir counts | `GET /v1/files/%2F/recursive-aggregates/` | `total_files`, `total_directories` from root entry |
| Snapshot count + space | `GET /v1/snapshots/total-used-capacity` + `GET /v2/snapshots/` | Total snapshot bytes + snapshot list (count = entries length) |
| IOPS/throughput (activity) | `GET /v1/analytics/activity/current?type=<type>` | Types: `file-iops-read`, `file-iops-write`, `file-throughput-read`, `file-throughput-write` |

Total: ~15 API calls per cluster. With 5 clusters = ~75 calls, but parallelized
per-cluster so wall clock time = slowest single cluster.

### NIC Throughput Calculation

The NIC stats from `/v3/network/status` provide cumulative `bytes_sent` and
`bytes_received` counters. To show current throughput:

**In one-shot mode:** Show cumulative bytes (less useful) OR make two calls
separated by 1 second and compute delta. Given the 2s watch interval,
prefer the delta approach in watch mode and show "N/A" or cumulative in
one-shot mode.

**In watch mode:** Store previous poll's byte counters, compute delta between
polls, divide by interval = throughput in bytes/sec.

**Alternative:** Use `/v1/analytics/time-series/` with
`throughput.read.rate` and `throughput.write.rate` for cluster-wide throughput
(but this is cluster-wide, not per-node). For per-node throughput, the NIC
counter delta is the only option.

**Recommendation:** In one-shot mode, make two NIC stats calls 1 second apart
to compute a snapshot of per-node throughput. In watch mode, use the delta
between polls.

### Link Speed

Per-node link speed comes from `/v3/network/status` → `devices[].speed` field.
Value is in Mbps (e.g., `"200000"` = 200 Gbps).

For on-prem clusters: show throughput as `X / Y Gbps` where Y is the node's
link speed, with utilization percentage.

For cloud clusters: show throughput only (link speed is opaque/fluid in cloud).

## 7. Cache Layer

### Location

`$XDG_CACHE_HOME/qontrol/status-cache.json` (defaults to `~/.cache/qontrol/status-cache.json`).

Overridable via `QONTROL_CACHE_DIR` environment variable (useful for testing).

### Schema

```json
{
  "version": 1,
  "clusters": {
    "<profile-name>": {
      "last_success": "2026-02-10T09:41:00Z",
      "data": {
        "cluster_name": "...",
        "version": "...",
        "cluster_type": "on-prem|cnq-aws|anq-azure",
        "nodes": [...],
        "capacity": {...},
        "files": {...},
        "snapshots": {...}
      }
    }
  }
}
```

### Behavior

- On successful poll: write cluster data + timestamp to cache
- On failed poll: read from cache, mark as stale in display
- `--no-cache` flag: skip all cache reads/writes
- Cache is per-profile-name, so renamed profiles start fresh

## 8. JSON Output Schema

When `--json` is passed, output a structured JSON object:

```json
{
  "timestamp": "2026-02-10T19:45:00Z",
  "aggregates": {
    "cluster_count": 5,
    "healthy_count": 4,
    "unreachable_count": 1,
    "total_nodes": 22,
    "online_nodes": 21,
    "offline_nodes": 1,
    "total_capacity_bytes": 2170000000000000,
    "used_capacity_bytes": 1860000000000000,
    "free_capacity_bytes": 310000000000000,
    "snapshot_bytes": 7700000000000,
    "total_files": 698412061,
    "total_directories": 48231004,
    "total_snapshots": 12847,
    "latency_min_ms": 8,
    "latency_max_ms": 142
  },
  "alerts": [
    {
      "severity": "critical",
      "cluster": "iss-sg",
      "message": "node 4: OFFLINE",
      "category": "node_offline"
    }
  ],
  "clusters": [
    {
      "profile": "gravytrain",
      "cluster_name": "gravytrain-sg",
      "cluster_uuid": "f83b970e-...",
      "version": "Qumulo Core 7.8.0",
      "cluster_type": "on-prem",
      "hardware_skus": ["C192T", "QCT_D52T"],
      "reachable": true,
      "stale": false,
      "latency_ms": 42,
      "nodes": {
        "total": 5,
        "online": 5,
        "offline": 0,
        "details": [
          {
            "id": 1,
            "name": "gravytrain-sg-1",
            "status": "online",
            "model_number": "C192T",
            "connections": 14,
            "connection_breakdown": {"NFS": 8, "SMB": 4, "REST": 2},
            "nic_throughput_bps": 12400000000,
            "nic_link_speed_bps": 200000000000,
            "nic_utilization_pct": 6.2
          }
        ]
      },
      "capacity": {
        "total_bytes": 605000000000000,
        "used_bytes": 594000000000000,
        "free_bytes": 11000000000000,
        "snapshot_bytes": 6700000000000,
        "used_pct": 98.2,
        "projection": {
          "growth_rate_bytes_per_day": 1200000000000,
          "days_to_full": 62,
          "confidence": "high"
        }
      },
      "activity": {
        "read_iops": 140,
        "write_iops": 122,
        "read_throughput_bps": 57800000,
        "write_throughput_bps": 1600000
      },
      "files": {
        "total_files": 501204881,
        "total_directories": 32401221,
        "total_snapshots": 8201
      },
      "health": {
        "disks_unhealthy": 0,
        "psus_unhealthy": 0,
        "data_at_risk": false,
        "remaining_node_failures": 1,
        "remaining_drive_failures": 2,
        "protection_type": "PROTECTION_SYSTEM_TYPE_EC"
      }
    }
  ]
}
```

## 9. Architecture

### Module Structure

```
src/commands/status.rs          — Command entry point, orchestration
src/commands/status/
    mod.rs                      — Re-exports
    collector.rs                — Multi-cluster parallel data collection
    types.rs                    — Data model structs (ClusterStatus, NodeStatus, etc.)
    health.rs                   — Health check logic + alert generation
    capacity.rs                 — Capacity projection (linear regression)
    detection.rs                — Cluster type detection
    cache.rs                    — XDG cache read/write
    renderer.rs                 — Formatted terminal output
    json.rs                     — JSON output serialization
```

### Multi-Cluster Client

```rust
// Pseudocode for parallel collection
fn collect_all(profiles: &[(String, ProfileEntry)], timeout: Duration)
    -> Vec<ClusterResult>
{
    let handles: Vec<_> = profiles.iter().map(|(name, profile)| {
        let name = name.clone();
        let profile = profile.clone();
        std::thread::spawn(move || {
            let start = Instant::now();
            match collect_cluster(&name, &profile, timeout) {
                Ok(data) => ClusterResult::Success {
                    profile: name,
                    data,
                    latency: start.elapsed(),
                },
                Err(e) => ClusterResult::Unreachable {
                    profile: name,
                    error: e,
                },
            }
        })
    }).collect();

    handles.into_iter().map(|h| h.join().unwrap()).collect()
}
```

Each thread creates its own `QumuloClient` and makes all ~15 API calls
sequentially within that thread. Clusters are fully independent.

### Error Isolation

Every API call within a cluster is individually caught. If one endpoint
fails (e.g., metrics returns 403), the rest of the cluster's data still
renders. Missing data is shown as `—` or omitted.

If the entire cluster is unreachable (connection refused, timeout), fall
back to cache.

### Latency Measurement

Wrap every `QumuloClient.request()` call with timing. After all calls
complete for a cluster, compute the average response time. Display in
the cluster header line as `<N>ms`.

## 10. Watch Mode

### Behavior

1. Collect all data (same as one-shot)
2. Render to string
3. Clear terminal (ANSI escape: `\x1b[2J\x1b[H`)
4. Print rendered output
5. Print footer: `Refreshing every 2s — press Ctrl+C to stop`
6. Sleep for interval
7. Goto 1

### NIC Throughput in Watch Mode

In watch mode, store the previous poll's NIC byte counters per node.
On subsequent polls, compute throughput as:

```
throughput = (current_bytes - previous_bytes) / interval_seconds
```

This gives accurate per-node throughput without needing a separate
measurement call.

### First Poll in Watch Mode

On the very first poll, per-node NIC throughput is unavailable (no
previous counters). Options:
- Show `—` for throughput on first render
- Make two rapid NIC calls 1s apart on first poll to bootstrap

Recommend: show `—` on first render, real data from second render onward.
Simpler and the user will see data within 2 seconds anyway.

## 11. Testing Strategy

**Testing is a first-class principle, not an afterthought.** Every piece of
code that ships must be tested appropriately. The level of testing should
match the risk and complexity of the change.

### Three-Tier Testing Model

#### Tier 1: Unit Tests (REQUIRED for all code)

Every module must have unit tests covering its core logic:

- Capacity projection: feed known data points, verify slope and days-to-full
- Cluster type detection: test model_number → type mapping
- Alert generation: test each alert condition with edge cases
- Cache serialization: round-trip test
- Renderer: snapshot tests against known data
- NIC throughput delta calculation
- Linear regression math
- Human-readable formatting (bytes → TB, etc.)

Unit tests run with `cargo test` and must pass before any PR is submitted.

#### Tier 2: Integration Tests with wiremock (REQUIRED for API-touching code)

Any code that talks to the Qumulo API MUST have wiremock-based integration
tests. The existing test harness (`tests/harness/`) already supports this
pattern — extend it for multi-cluster scenarios.

Required coverage:
- Mock multiple clusters with different states (healthy, degraded, mixed types)
- Test unreachable cluster fallback to cache
- Test mixed cluster types (on-prem + cloud in same run)
- Test partial API failures (one endpoint 403s, rest succeed)
- Test JSON output schema validation
- Test `--profile` filtering

**Fixture recording:** When building API abstractions, record REAL API
responses from the configured clusters and save them as test fixtures in
`tests/fixtures/`. This ensures parsers handle real-world response shapes,
not idealized ones. Each cluster type (on-prem, CNQ/AWS, ANQ/Azure) should
have its own fixture set.

#### Tier 3: Live Validation (RECOMMENDED for API-touching code, USE JUDGMENT)

The development host has 5 real production clusters configured in
`~/.config/qontrol/config.toml`. When a polecat is building or modifying
code that interacts with real APIs, they SHOULD validate against live
clusters before submitting.

**When to do live validation:**
- New API client code or parsers — verify real responses match expectations
- Changes to data collection logic — confirm real clusters return expected data
- Renderer changes where real data might expose edge cases (large numbers,
  empty arrays, unexpected states)
- Any time wiremock tests pass but you're not confident the real API matches

**When live validation is NOT needed:**
- Pure logic changes (algorithms, math, formatting)
- Renderer-only changes tested with snapshot tests
- Cache layer changes (file I/O, no API interaction)
- CLI argument parsing changes
- Refactoring that doesn't change behavior

**How to validate live:**
- Build in release mode: `cargo build --release`
- Run against a single cluster: `./target/release/qontrol --profile gravytrain status`
- Run against all clusters: `./target/release/qontrol status`
- Compare output to expectations — real clusters may have unexpected states
  (nodes rebooting, high load, etc.) which is GOOD for finding edge cases
- If you discover an edge case from live testing, add a wiremock fixture
  that reproduces it so it's permanently covered

**Caution:** These are real production clusters. Read-only API calls only.
Do not create, modify, or delete anything. All endpoints used by `status`
are read-only GETs, so this should be safe.

### Testing Mandate for Issues

Each issue in this epic will be tagged with its required testing tier:

- `[T1]` — Unit tests required
- `[T1+T2]` — Unit tests + wiremock integration tests required
- `[T1+T2+T3]` — All three tiers; includes live validation
- `[T1 only]` — Unit tests sufficient (no API interaction)

Polecats MUST NOT submit work as done unless the required testing tier
is satisfied. `cargo test` must pass. If live validation is recommended
and reveals issues, those must be fixed or filed as follow-up issues
before completion.

### Quality Gate

The existing CI quality gate (`just ci`) must pass. All new tests are
additive — they must not break existing tests. Test coverage should be
meaningful, not just line-count padding.

## 12. Migration

### Replacing `dashboard`

1. Rename current `dashboard` module to `dashboard_legacy` (or delete)
2. Register `status` as the primary command
3. Register `dashboard` as an alias for `status`
4. Update CLI help text

### Config Changes

No config changes needed. The existing profile configuration already
has everything `status` needs. Cache is created automatically on first run.

## Appendix A: Full API Response Shapes

See research findings on issues qo-axf (API inventory), qo-g8m (metrics +
cluster detection), and qo-4qm (codebase audit) for complete API response
examples and cross-cluster compatibility matrix.

## Appendix B: Cluster Inventory (Test Environment)

| Profile | Cluster Name | Type | Nodes | Capacity | Version |
|---------|-------------|------|-------|----------|---------|
| gravytrain | gravytrain-sg | On-prem (C192T + QCT) | 5 | 665 TB | 7.8.0 |
| aws-gravytrain | aws-gravytrain | CNQ/AWS | 3 | 500 TB | 7.8.0 |
| az-gravytrain | gravytrain-east | ANQ/Azure | 3 | 500 TB | 7.8.0 |
| iss | iss-sg | On-prem | 6 | 809 TB | 7.8.0 |
| music | music | On-prem | 5 | 72 TB | 7.8.0 |

# fill_summary_index_rest.py Usage Guide

This script backfills summary-index data by executing saved searches at their historical scheduled times, using Splunk REST API endpoints (not the Splunk Python SDK).

## What it does

- Connects to Splunk management API on port 8089 (Splunk Cloud supported).
 Uses HTTP Basic authentication with username/password, or an existing session key.
 `-auth <string>` Username or username:password (HTTP Basic Auth)
 `-sk <string>` Session key override (if you prefer Splunk session-key auth)
- Optionally skips runs that already exist in summary data (`-dedup true`).
  - `-auth <user:pass>` or `-auth <user>` (password prompt) for HTTP Basic Auth, or

### 3) Use session key instead of HTTP Basic Auth

- Python 3.8+.
- Network access to Splunk management endpoint (usually `https://<host>:8089`).
- A Splunk user with permission to:
  - Read saved searches.
  - Dispatch searches/jobs.
  - Read job status/results.

## Splunk Cloud host requirement

For Splunk Cloud, always provide the management host using `-host`.

Examples:

- `-host https://prd-p-xxxxx.splunkcloud.com:8089`
- `-host prd-p-xxxxx.splunkcloud.com` (script auto-adds `https://` and `:8089`)

## Basic syntax

```bash
python fill_summary_index_rest.py [OPTIONS]
```

## Required options (practical minimum)

- `-host <string>` Splunk management host (critical for Splunk Cloud).
- `-app <string>` app context containing the saved searches.
- `-et <string>` earliest time.
- `-lt <string>` latest time.
- At least one saved search selector:
  - `-name <string>` (repeatable), or
  - `-names <csv>`, or
  - `-namefile <file>`, or
  - `-name "*"` for all enabled+scheduled searches with `summary_index` action.
- Authentication:
  - `-auth <user:pass>` or `-auth <user>` (password prompt), or
  - `-sk <session_key>`

## Common examples

### 1) Backfill one saved search

```bash
python fill_summary_index_rest.py \
  -host https://prd-p-xxxxx.splunkcloud.com:8089 \
  -app search \
  -name my_daily_summary \
  -et -30d@d \
  -lt now \
  -auth admin:changeme
```

### 2) Backfill all eligible summary searches with dedup

```bash
python fill_summary_index_rest.py \
  -host prd-p-xxxxx.splunkcloud.com \
  -app search \
  -name "*" \
  -et -7d@d \
  -lt now \
  -dedup true \
  -j 4 \
  -showprogress true \
  -auth admin:changeme
```

### 3) Use session key instead of username/password

```bash
python fill_summary_index_rest.py \
  -host https://prd-p-xxxxx.splunkcloud.com:8089 \
  -app search \
  -names "search_a,search_b" \
  -et -1mon@mon \
  -lt @mon \
  -sk <SESSION_KEY>
```

### 4) Read search names from a file

```bash
python fill_summary_index_rest.py \
  -host https://prd-p-xxxxx.splunkcloud.com:8089 \
  -app search \
  -namefile saved_searches.txt \
  -et -14d@d \
  -lt now \
  -auth admin
```

`saved_searches.txt` format:

```text
my_search_1
my_search_2
# this is a comment
my_search_3
```

## Important options

- `-owner <string>` search owner context (default: `nobody`).
- `-j <int>` max concurrent jobs (1..16).
- `-sleep <float>` polling interval in seconds (default `5`).
- `-trigger <boolean>` trigger actions during dispatch (default `true`).
- `-reverseorder <boolean>` run newest scheduled time first.
- `-showprogress <boolean>` show progress while polling jobs.
- `-sched_start_time <HHMM>` local-time window start.
- `-sched_end_time <HHMM>` local-time window end.
- `-index <string>` summary index override.
- `-dedupsearch <string>` custom dedup SPL template.
- `-namefield <string>` field holding saved search name in summary events.
- `-timefield <string>` field holding scheduled timestamp in summary events.
- `-nolocal true` switch dedup template to distributed search variant.
- `-ca-file <path>` trust a private or self-signed CA certificate bundle.

Boolean accepted values:

- True: `1`, `t`, `true`, `yes`
- False: `0`, `f`, `false`, `no`

## Time values

`-et` and `-lt` support:

- Unix epoch timestamps.
- Splunk relative time strings, for example:
  - `-24h@h`
  - `-7d@d`
  - `-1mon@mon`
  - `@mon`
  - `now`

## Troubleshooting

- Login fails:
  - Verify credentials and role capabilities.
  - Verify host points to management API endpoint.
- TLS/certificate errors:
  - Confirm trust chain for your endpoint.
  - For a private or self-signed CA, provide its certificate bundle with `-ca-file <path>`.
- No searches found with `-name "*"`:
  - Search must be enabled, scheduled, and include `summary_index` action.
  - Check app/owner scope.
- No scheduled times found:
  - Validate `-et` / `-lt` window and saved search schedule.
- No work to run with dedup:
  - Existing summary events may already cover the selected schedule times.

## Exit behavior and lock

- The script creates a lock file in system temp to prevent multiple concurrent runs for the same host+app combination.
- Lock is cleaned up on normal process exit.

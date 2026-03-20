# dbt-cost

Estimate BigQuery costs for dbt models **before** you run them.

dbt-cost uses BigQuery's free [dry-run API](https://cloud.google.com/bigquery/docs/samples/bigquery-query-dry-run) to predict bytes scanned for each model, converts that to dollars, and gives you a clear cost breakdown — in your terminal, as JSON for CI, or as a markdown comment on your PR.

## Install

```bash
pip install dbt-cost
```

Requires Python 3.9+ and a compiled dbt manifest (`dbt compile`).

## Quick Start

```bash
# Cost of a single model
dbt-cost estimate dim_customers

# Cost of a model + all downstream
dbt-cost estimate stg_orders+

# Full project cost report
dbt-cost report

# Compare costs between two branches (for PRs)
dbt-cost diff --base-manifest base_manifest.json --pr-manifest pr_manifest.json
```

## Commands

### `estimate`

Estimate the cost of specific models using [dbt selector syntax](https://docs.getdbt.com/reference/node-selection/syntax).

```bash
# Single model
dbt-cost estimate dim_customers

# Model + downstream
dbt-cost estimate stg_orders+

# Model + 2 levels downstream
dbt-cost estimate stg_orders+2

# Upstream + model + downstream
dbt-cost estimate +dim_customers+

# By tag
dbt-cost estimate --selector tag:marketing

# Top 5 most expensive, JSON output
dbt-cost --format json estimate stg_orders+ --top 5
```

**Options:**
| Flag | Default | Description |
|------|---------|-------------|
| `--price` | `6.25` | Price per TB in USD (BigQuery on-demand) |
| `--concurrency` | `10` | Number of concurrent dry-runs |
| `--top` | all | Show only the N most expensive models |

### `report`

Full project cost breakdown by layer.

```bash
dbt-cost report
dbt-cost report --top 20
dbt-cost report --layer staging
dbt-cost --format json report
```

**Options:**
| Flag | Default | Description |
|------|---------|-------------|
| `--top` | `10` | Show top N most expensive models |
| `--layer` | all | Filter to a specific layer |
| `--price` | `6.25` | Price per TB in USD |
| `--concurrency` | `10` | Concurrent dry-runs (max 50) |

### `diff`

Compare costs between two manifests — designed for CI pipelines and PR reviews.

```bash
# Basic diff
dbt-cost diff --base-manifest target/base.json --pr-manifest target/pr.json

# Markdown output for PR comments
dbt-cost --format markdown diff --base-manifest base.json --pr-manifest pr.json

# Fail CI if cost increase exceeds $50
dbt-cost diff --base-manifest base.json --pr-manifest pr.json --threshold 50
```

**Example markdown output:**

```
dbt-cost: Cost Impact of this PR

| Model | Before | After | Change |
|-------|--------|-------|--------|
| dim_customers | $0.80 | $45.00 | +$44.20 (+5,525%) |
| stg_orders | $2.10 | $2.10 | no change |

Total: $2.90 -> $47.10 (+$44.20)

2 models changed, 0 added, 0 removed
```

**Options:**
| Flag | Default | Description |
|------|---------|-------------|
| `--base-manifest` | required | Path to base branch manifest.json |
| `--pr-manifest` | required | Path to PR branch manifest.json |
| `--threshold` | none | Max allowed cost increase in USD; exit 1 if exceeded |
| `--include-unchanged` | `false` | Include unchanged models in output |
| `--price` | `6.25` | Price per TB in USD |
| `--concurrency` | `10` | Concurrent dry-runs |

### Global Options

These apply to all commands:

| Flag | Default | Description |
|------|---------|-------------|
| `--manifest` | `target/manifest.json` | Path to dbt manifest |
| `--credentials` | ADC | Path to GCP service account key JSON |
| `--format` | `table` | Output format: `table`, `json`, or `markdown` |

## GitHub Actions

Add automated cost checks to your PRs. Copy [`examples/dbt-cost-pr.yml`](examples/dbt-cost-pr.yml) to your project's `.github/workflows/` directory.

The workflow:
1. Compiles dbt on both the base and PR branches
2. Runs `dbt-cost diff` to compare costs
3. Posts a sticky PR comment with the cost impact
4. Fails the check if cost increase exceeds your threshold

## Authentication

dbt-cost needs read-only BigQuery access to run dry-runs. It supports:

1. **Application Default Credentials** (recommended) — run `gcloud auth application-default login`
2. **Service account key** — pass `--credentials /path/to/key.json`

Required IAM permissions: `bigquery.jobs.create` (BigQuery Job User role) and read access to referenced tables.

**Safety:** dry-run mode is hardcoded — dbt-cost can never execute queries.

## How It Works

```
manifest.json (from dbt compile)
       |
   dbt-lens parses the manifest + dependency graph
       |
   Selector resolves which models to estimate
       |
   BigQuery dry-run API returns bytes_processed (free, no execution)
       |
   bytes / 1 TB * $6.25 = estimated cost
       |
   Table, JSON, or Markdown output
```

## Development

```bash
git clone https://github.com/ishanyash/dbt-cost.git
cd dbt-cost
pip install -e ".[dev]"

# Run checks
ruff check src/ tests/
mypy src/dbt_cost/
pytest tests/ -v
```

## License

MIT

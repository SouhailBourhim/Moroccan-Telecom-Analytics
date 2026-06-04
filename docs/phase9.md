# Phase 9 — Metabase Dashboards

**Status:** ❌ Incomplete — blocked by Docker/Metabase driver compatibility issues

## What was attempted

### Goal
Connect Metabase to `data/warehouse.duckdb` and create 5 dashboards:
1. Market Overview (mobile/fixed/internet KPIs by quarter)
2. Operator Performance (market share, ARPM, traffic)
3. QoS Scorecard (quality indicators over time)
4. Internet Evolution (technology mix, broadband penetration)
5. Morocco Benchmarks (ITU indicators 2000–2024)

### Changes made to docker-compose.yml
```yaml
metabase:
  image: metabase/metabase:v0.49.7   # pinned from 'latest' for driver compat
  environment:
    MB_PLUGINS_DIR: /plugins          # added
  volumes:
    - ./metabase-plugins:/plugins:ro  # added — mounts driver JAR
    - ./data:/opt/data:ro
```

### DuckDB driver downloaded
`metabase-plugins/duckdb.metabase-driver.jar` — AlexR2D2 community driver v0.2.4 (61 MB)

## Problems encountered

### 1. `metabase/metabase:latest` is v0.61.3.3 — incompatible with driver
**Problem:** The `latest` image pulled was Metabase v0.61.3.3. The community DuckDB driver v0.2.4 was built for Metabase ~0.49. The driver JAR is placed in `/plugins` but Metabase 0.61 can't load it:
```
Could not locate metabase/driver/duckdb__init.class...
on classpath. The system-classpath only includes /app/metabase.jar
```
The plugin system in 0.61 changed how JARs are loaded from the plugins directory.

**Attempted fix:** Switched image to `metabase/metabase:v0.49.7` (ARM64 warning, but should work via QEMU emulation).

### 2. Docker Desktop crashed during Metabase v0.49.7 startup
**Problem:** While waiting for Metabase v0.49.7 to start, Docker Desktop became unresponsive (`Error response from daemon: Docker Desktop is unable to start`). All containers were stuck and could not be killed or removed.

**Status:** User asked to stop and kill all Docker processes. Docker Desktop needs a full restart before retrying.

### 3. Metabase admin credentials lost after volume reset
**Problem:** The initial `latest` Metabase instance had been set up with unknown credentials (persisted in H2 volume from a previous session). Had to delete the volume and recreate with fresh credentials:
- Email: `admin@telecom.ma`
- Password: `Admin1234!`

Setup was completed via the Metabase Setup API (`POST /api/setup`).

## What needs to be done to complete

1. **Restart Docker Desktop** cleanly
2. **Start services** with `docker compose up -d`
3. **Verify Metabase v0.49.7 starts** with the DuckDB driver loaded (check logs for `Registered driver :duckdb`)
4. **Create DuckDB database connection** via API:
   ```json
   POST /api/database
   {"engine": "duckdb", "name": "Morocco Telecom Warehouse",
    "details": {"db": "/opt/data/warehouse.duckdb"}}
   ```
5. **Create 5 dashboards** via Metabase UI or API using the Gold mart tables:
   - `mart_market_overview`
   - `mart_operator_perf`
   - `mart_qos_scorecard`
   - `mart_internet_evol`
   - `mart_benchmarks`

## Files in place
- `docker-compose.yml` — updated with `v0.49.7` image and plugins mount
- `metabase-plugins/duckdb.metabase-driver.jar` — driver JAR ready (61 MB, gitignored)

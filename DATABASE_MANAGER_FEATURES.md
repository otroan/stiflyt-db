# Database Manager Component - Recommended Features

## Current Status ✅

The package currently handles:
- ✅ Downloading datasets
- ✅ Loading datasets into PostGIS
- ✅ Automated updates via cron
- ✅ Logging

## Recommended Additions

### 1. **Database Health & Status** 🔴 HIGH PRIORITY

**Purpose**: Monitor database health, verify updates succeeded, check for issues

**Features**:
- Database connection test
- PostGIS extension verification
- Table existence verification
- Row count verification (ensure data loaded)
- Database size monitoring
- Last update timestamp tracking

**Use cases**:
- Verify updates completed successfully
- Alert if database is empty or corrupted
- Monitor database growth
- Health checks before allowing backend connections

**Script**: `scripts/db_status.py`
```bash
python3 scripts/db_status.py [database]
# Output:
# Database: matrikkel
# Status: ✓ Connected
# PostGIS: ✓ Enabled (version 3.3)
# Tables: 5
# Total rows: 1,234,567
# Database size: 2.3 GB
# Last update: 2024-01-15 02:15:00
```

### 2. **Schema Inspection** 🟡 MEDIUM PRIORITY

**Purpose**: List tables, columns, indexes, spatial reference systems

**Features**:
- List all tables with row counts
- Show table schemas (columns, types)
- List spatial indexes
- Show SRIDs in use
- Table size breakdown

**Use cases**:
- Debugging schema issues
- Documentation
- Verifying expected tables exist
- Performance analysis

**Script**: `scripts/inspect_db.py`
```bash
python3 scripts/inspect_db.py [database] [--tables] [--schema TABLE] [--indexes]
```

### 3. **Data Validation** 🟡 MEDIUM PRIORITY

**Purpose**: Verify loaded data integrity

**Features**:
- Row count verification (compare with expected)
- Geometry validation (check for invalid geometries)
- Spatial extent verification
- Data freshness check (max timestamp)
- Missing data detection

**Use cases**:
- Post-load verification
- Quality assurance
- Alert on data corruption
- Validate updates succeeded

**Script**: `scripts/validate_data.py`
```bash
python3 scripts/validate_data.py [database] [--table TABLE]
```

### 4. **Database Maintenance** 🟡 MEDIUM PRIORITY

**Purpose**: Keep database optimized and healthy

**Features**:
- VACUUM ANALYZE (update statistics)
- REINDEX (rebuild indexes)
- Check for bloat
- Update table statistics
- Spatial index maintenance

**Use cases**:
- Post-update optimization
- Performance maintenance
- Prevent bloat
- Keep query planner accurate

**Script**: `scripts/maintain_db.py`
```bash
python3 scripts/maintain_db.py [database] [--vacuum] [--analyze] [--reindex]
```

### 5. **Backup Utilities** 🟢 LOW PRIORITY (but important)

**Purpose**: Backup database before updates

**Features**:
- pg_dump backup before update
- Backup rotation (keep N backups)
- Restore from backup
- Backup verification

**Use cases**:
- Safety net before updates
- Disaster recovery
- Rollback capability

**Script**: `scripts/backup_db.py`
```bash
python3 scripts/backup_db.py [database] [--backup-dir ./backups]
```

### 6. **Update Verification** 🔴 HIGH PRIORITY

**Purpose**: Verify updates completed successfully

**Features**:
- Check all expected tables exist
- Verify row counts are reasonable (not zero, not suspiciously low)
- Check update timestamps
- Compare with previous state
- Alert on failures

**Use cases**:
- Post-update verification
- Automated health checks
- Alerting system integration

**Integration**: Add to `update_datasets.py` as post-update step

### 7. **Monitoring & Alerting** 🟡 MEDIUM PRIORITY

**Purpose**: Track database metrics over time

**Features**:
- Database size tracking
- Update success/failure tracking
- Performance metrics
- Export metrics for monitoring systems (Prometheus, etc.)

**Use cases**:
- Long-term monitoring
- Capacity planning
- Performance tracking
- Alerting integration

**Script**: `scripts/monitor_db.py`
```bash
python3 scripts/monitor_db.py [database] [--export-json] [--export-prometheus]
```

## Priority Recommendations

### Must Have (for production):
1. **Database Status** - Verify updates succeeded
2. **Update Verification** - Post-update checks
3. **Schema Inspection** - Debugging and verification

### Should Have:
4. **Data Validation** - Quality assurance
5. **Database Maintenance** - Performance optimization

### Nice to Have:
6. **Backup Utilities** - Safety net
7. **Monitoring** - Long-term tracking

## Implementation Suggestions

### Quick Wins (can implement now):

1. **db_status.py** - Simple connection and table check
   - ~100 lines
   - Uses existing database connection code
   - Can be integrated into update script

2. **inspect_db.py** - Schema listing
   - ~150 lines
   - Uses psycopg2 queries
   - Useful for debugging

3. **Update verification** - Add to `update_datasets.py`
   - ~50 lines
   - Post-update checks
   - Prevents silent failures

### Integration Points:

- **update_datasets.py**: Add post-update verification
- **Makefile**: Add targets for status, inspect, maintain
- **Cron**: Add health check cron job

## Example Integration

```python
# In update_datasets.py, after loading:
from db_status import check_database_health

if check_database_health(database):
    log("✓ Database health check passed", log_file)
else:
    log("✗ Database health check failed - alert!", log_file)
    sys.exit(1)
```

## Benefits

1. **Reliability**: Catch failures early
2. **Debugging**: Easy inspection tools
3. **Monitoring**: Track database health over time
4. **Confidence**: Verify updates succeeded
5. **Performance**: Maintain database optimization


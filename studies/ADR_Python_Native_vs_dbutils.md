# Architecture Decision Record (ADR): Python Native vs dbutils.fs

## Context
The Brazilian Legislative Data Pipeline needs to persist data from the Câmara API to Unity Catalog Volumes for Bronze layer storage.

## Decision
**Use Python native file operations (`os`, `open()`) instead of `dbutils.fs`**

## Status
✅ Accepted and Implemented

---

## Rationale

### 1. Unity Catalog Volumes Architecture
Unity Catalog Volumes implements **POSIX filesystem** via **FUSE (Filesystem in Userspace)**:
- Volumes are mounted as local paths: `/Volumes/catalog/schema/volume/`
- Python can access them directly like any filesystem
- No need for Databricks-specific APIs

**Source:** [Databricks Documentation - Work with files in Unity Catalog volumes]
> "You can also use OSS packages for file utility commands, such as the Python os module"

### 2. Consistency with PySpark Ecosystem
- **Spark is Python-first**: PySpark is the primary API
- **DataFrames read/write**: Already using `spark.read.table()` and `df.write.saveAsTable()`
- **Pythonic**: Follows Python idioms and conventions

### 3. Testability & Development Velocity
| Aspect | Python Native | dbutils.fs |
|--------|---------------|------------|
| **Unit Tests** | ✅ Run locally with mocks | ❌ Requires Databricks env |
| **IDE Support** | ✅ Full autocomplete | ⚠️ Limited |
| **Debugging** | ✅ Standard Python debugger | ⚠️ Notebook-only |
| **CI/CD** | ✅ GitHub Actions compatible | ❌ Needs Databricks Connect |

### 4. Code Maintainability
```python
# Python Native (Universal)
import os
os.makedirs(path, exist_ok=True)
with open(file_path, 'w') as f:
    json.dump(data, f)

# dbutils.fs (Databricks-specific)
dbutils.fs.mkdirs(path)
dbutils.fs.put(file_path, json.dumps(data), overwrite=True)
```

**Onboarding Time:**
- Python native: ~0 minutes (any Python dev knows it)
- dbutils.fs: ~1-2 hours (requires Databricks training)

### 5. Vendor Lock-in Mitigation
While we're committed to Databricks, using standard Python:
- **Reduces coupling** to Databricks-specific APIs
- **Enables portability** (code runs anywhere with PySpark)
- **Future-proofs** against API changes

---

## Implementation

### Before (Coupled to Databricks)
```python
class BaseBronzeIngestion:
    def __init__(self, spark, dbutils, entity_name):
        self.spark = spark
        self.dbutils = dbutils  # ← Databricks dependency

def save_to_volume(config, data, spark, dbutils):
    dbutils.fs.mkdirs(config.landing_path)  # ← Databricks API
    with open(file_path, 'w') as f:
        json.dump(data, f)
```

### After (Python Native)
```python
import os

class BaseBronzeIngestion:
    def __init__(self, spark, entity_name):
        self.spark = spark  # ← Only Spark needed

def save_to_volume(config, data, spark):
    os.makedirs(config.landing_path, exist_ok=True)  # ← Python stdlib
    with open(file_path, 'w') as f:
        json.dump(data, f)
```

---

## Consequences

### Positive
✅ **Testability**: Can write unit tests that run locally  
✅ **Simplicity**: 2 parameters instead of 3 (`spark, entity_name` vs `spark, dbutils, entity_name`)  
✅ **Performance**: Direct POSIX access, no API translation layer  
✅ **Standards**: Follows Python/Spark community practices  
✅ **Portability**: Code runs in any PySpark environment  

### Negative
⚠️ **Limited to UC Volumes**: If we need direct cloud storage access (S3/Azure), would need refactoring  
⚠️ **DBFS Root not supported**: Old DBFS paths (`/dbfs/...`) would require dbutils  

### Mitigation
If future requirements need direct cloud access, we can:
1. **Preferred**: Continue using Unity Catalog Volumes (abstracts storage)
2. **Alternative**: Implement a `StorageProvider` interface with multiple backends

---

## Alternative Considered

### Option: dbutils.fs Everywhere
**Pros:**
- Works with any Databricks storage (DBFS, S3, Azure)
- Single API for all file operations

**Cons:**
- Requires dbutils in all layers (violates separation of concerns)
- Not testable outside Databricks
- Inconsistent with PySpark patterns
- Steeper learning curve for new team members

**Conclusion:** Rejected because Unity Catalog Volumes + Python native achieves the same goals with better DX.

---

## References
- [Databricks Docs: Work with files in Unity Catalog volumes](https://docs.databricks.com/volumes/volume-files/)
- [Unity Catalog Volumes Architecture](https://docs.databricks.com/volumes/)
- PEP 8 - Python Style Guide
- Clean Architecture (Robert C. Martin) - Dependency Rule

---

## Decision Makers
- Architecture: Data Engineering Team
- Reviewed: 2026-05-12
- Status: ✅ Implemented

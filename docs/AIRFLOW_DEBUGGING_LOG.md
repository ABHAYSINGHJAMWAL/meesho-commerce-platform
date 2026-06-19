# Airflow + PySpark Integration — Debugging Log

Real production-style debugging session getting PySpark to run
inside Airflow via Docker (Astro CLI) on Windows.

## Issues Found and Fixed

### 1. Deprecated Airflow context key
**Error:** `execution_date` access pattern deprecated in newer Airflow
**Fix:** Changed `context['execution_date']` → `context['logical_date']`
**Why:** Airflow 2.2+ renamed this concept for clarity — logical_date
represents the data interval being processed, not wall-clock execution time.

### 2. PySpark not installed in Airflow container
**Fix:** Added `pyspark==3.5.0` to requirements.txt
**Why:** BashOperator runs Python scripts inside the Airflow container's
environment — packages must be available there, not just on host machine.

### 3. Dependency conflict — colorlog vs Airflow
**Fix:** Removed `colorlog==6.7.0` from requirements.txt
**Why:** Airflow pins specific versions of logging-related dependencies
internally. Adding conflicting versions breaks the container build.

### 4. PyArrow version incompatibility
**Fix:** Updated pyarrow to a version compatible with Python 3.13
**Why:** PyArrow ships pre-compiled wheels per Python version. Pinning
an old version that predates your Python version causes install failure.

### 5-6. Java not installed / JAVA_HOME not set
**Fix:** Added `default-jdk` to packages.txt, set:
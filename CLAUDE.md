# CLAUDE.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

# Growth Machine (GTM)

Growth Machine is a collection of automations for DevTool companies building out their Growth function. It's a hybrid application combining a Reflex web frontend with sophisticated data processing pipelines powered by Modal.com, Google Cloud AI, and vector databases.

## Architecture Overview

### Web Frontend

The web application is built with **Reflex** (Python-based React framework):
- Entry point: `web/web.py` - Main app configuration
- Pages: `web/pages/index/page.py` - Frontend components
- Configuration: `rxconfig.py` - Reflex app settings

### Core Data Services (`src/services/`)

- **`dlt/`**: Data loading and transformation layer with support for local and GCP destinations
- **`gemini/`**: Google Vertex AI text embedding service integration (text-embedding-005 model)
- **`lance/`**: LanceDB vector database client and upload utilities
- **`runner/`**: Modal.com orchestration for ETL pipelines and webhook processing
- **Integration Services**: `clay/`, `fathom/`, `octolens/` - Third-party data source connectors

### Modal.com Serverless Architecture

The application uses Modal for:
- Serverless webhook endpoints (`@modal.fastapi_endpoint`)
- ETL job orchestration with volume storage
- Batch processing with automatic scaling
- Secret management for API keys

### Analytics Layer

- **dbt project**: `dbt_project.yml` configures analytics transformations
- Project ID: `70471823493793` for dbt Cloud integration

## Development Commands

### Web Application

```bash
# Start Reflex development server
reflex run

# Initialize new Reflex project structure
reflex init
```

### Testing

```bash
# Run all tests (includes embedded tests in all .py files)
pytest

# Run tests with verbose output
pytest -v

# Run specific test patterns
pytest -k "test_destination_type"

# Run integration tests only
pytest -k "integration_test_"

# Run tests in specific directory
pytest src/services/dlt/
```

### Code Quality

```bash
# Check all linting rules
trunk check

# Format code
trunk fmt

# Check specific files
trunk check src/services/gemini/embed.py
```

### Modal.com Deployment & Execution

```bash
# Deploy Modal app
modal deploy src/services/runner/export_to_lancedb.py

# Run local entrypoint
modal run src/services/runner/export_to_lancedb.py::local --input-folder /path/to/data

# Serve Modal endpoint locally
modal serve src/services/runner/export_to_lancedb.py
```

### DBT Transformations

```bash
# Run dbt transformations
dbt run --profiles-dir ./profiles

# Test dbt models
dbt test --profiles-dir ./profiles
```

## Testing Architecture

This project uses a unique testing approach:

### Co-located Tests

- Tests are embedded **at the bottom** of the same files they test
- All `.py` files are scanned for test functions (configured in `pyproject.toml`)
- Test functions follow patterns: `test_*` and `integration_test_*`

### Test Organization

```python
# At the bottom of each .py file:
# trunk-ignore-begin(ruff/ANN002,ruff/ANN003,ruff/BLE001,ruff/PLC0415,ruff/PLR0912,ruff/PLR0915,ruff/PLR2004,ruff/S101)

def test_function_name() -> None:
    """Test description."""
    # Test implementation

# trunk-ignore-end(ruff/ANN002,ruff/ANN003,ruff/BLE001,ruff/PLC0415,ruff/PLR0912,ruff/PLR0915,ruff/PLR2004,ruff/S101)
```

### Test Collection Configuration

Pytest is configured to:
- Search all directories except exclusions (`lib`, `.venv`, `.trunk`, etc.)
- Collect from all Python files (not just `test_*.py`)
- Exclude specific files via `conftest.py`: webhook handlers and CSV processors

## Environment & Configuration

### Required Environment Variables

```bash
export GCP_PROJECT_ID="your-gcp-project"
export LANCEDB_API_KEY="your-lancedb-key"
# Additional secrets managed via Modal
```

### GCP Configuration

- **Project**: chalk-lab
- **Region**: us-east1 (primary)
- **Zone**: us-east1-c
- Integration with Vertex AI for embeddings

### Key Dependencies

- `reflex` - Web framework
- `modal` - Serverless orchestration
- `lancedb` - Vector database
- `google-cloud-aiplatform` - AI services
- `dbt-bigquery` - Analytics transformations
- `dlt` - Data loading
- `polars` - High-performance data processing

### Configuration Files

- `rxconfig.py` - Reflex app configuration
- `dbt_project.yml` - Analytics project settings
- `.trunk/trunk.yaml` - Code quality tools
- `pyproject.toml` - Python project and test configuration

## Code Quality Standards

### Linting & Formatting

- **Primary**: `ruff` for Python linting and formatting
- **Type Checking**: `pyright` for static analysis
- **Security**: Multiple scanners via Trunk (checkov, semgrep, trufflehog)
- **Configuration**: Exported configs from `oss-linter-trunk` plugin

### Code Conventions

- All files start with `from __future__ import annotations`
- Comprehensive type annotations required
- Pydantic models for data validation
- Enum-based configuration patterns

### Import Standards

```python
from __future__ import annotations

from typing import TYPE_CHECKING

# Standard library imports
# Third-party imports
# Local imports

if TYPE_CHECKING:
    # Type-only imports
```

## Data Processing Pipeline

### ETL Workflow

1. **Data Ingestion**: Webhooks processed via Modal endpoints
2. **Embedding Generation**: Vertex AI text-embedding-005 with batch size limits (250)
3. **Deduplication**: LanceDB primary key checking to avoid re-processing
4. **Vector Storage**: Batch upload to LanceDB with configurable delays
5. **Error Handling**: Comprehensive retry logic and rate limiting

### Batch Processing

- **Gemini Embedding Batch Size**: 250 (API maximum for text-embedding-005)
- **Upload Delay**: 0.1 seconds between batches to prevent rate limiting
- **Retry Logic**: 3 maximum attempts for rate-limited operations

### Storage Patterns

- **Local**: `./out/{bucket_name}` directory structure
- **GCP**: `gs://{bucket_name}` Cloud Storage buckets
- **Modal Volumes**: Persistent storage for serverless functions

### Webhook Processing

Modal endpoints handle:
- Webhook validation via Pydantic models
- Automatic data transformation and embedding
- Batch processing with memory snapshots disabled
- Concurrent request limiting (`max_inputs=1`)

## Tips & Common Pitfalls

### Environment Setup

- Ensure GCP credentials are properly configured for Vertex AI access
- LanceDB API keys must be set in environment variables
- Modal secrets should be configured before deployment

### Testing Best Practices

- Run tests frequently as they're embedded throughout the codebase
- Pay attention to trunk-ignore blocks - they disable specific linting rules
- Mock external services (GCP, LanceDB) in tests for reliability

### Performance Considerations

- Respect Gemini API batch limits (250 items max)
- Use appropriate delays between LanceDB uploads
- Consider memory usage with large datasets in Modal functions

### Modal Deployment

- Volumes must exist before referencing in functions
- Secret names must match exactly in Modal dashboard
- Check Modal logs for webhook debugging
- Local testing with `modal serve` before deployment

### Debugging Webhooks

- Use Modal's built-in docs endpoint for testing
- Validate webhook payloads against Pydantic models
- Check LanceDB for duplicate prevention logic
- Monitor embedding batch processing logs

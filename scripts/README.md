# Scripts

## create_lancedb_index.py

Standalone script to create a scalar index on a LanceDB table's primary key column.

### Purpose

This script fixes the error:
```
HttpError: Bad request: Merge insert cannot be performed because the number
of un-indexed rows (10001) exceeds the maximum of 10000. Please create an
index on the join column id
```

### Prerequisites

1. **LanceDB API Key**: Set the `LANCEDB_API_KEY` environment variable:
   ```bash
   export LANCEDB_API_KEY='your-api-key-here'
   ```

2. **Python Environment**: Ensure you're in the project's virtual environment:
   ```bash
   source .venv/bin/activate
   ```

### Usage

Run the script from the project root:

```bash
python scripts/create_lancedb_index.py
```

### What It Does

1. Reads configuration from `WebhookModel` (marketplace products table)
2. Connects to LanceDB project `marketplace-x205j4`
3. Opens table `marketplace_products`
4. Creates a BTREE scalar index on the `id` column
5. Waits up to 15 minutes for index creation to complete

### Expected Output

```
================================================================================
LanceDB Index Creation Script
================================================================================

Configuration:
  Project:     marketplace-x205j4
  Table:       marketplace_products
  Column:      id
  Index Type:  BTREE

Connecting to LanceDB...
✓ Connected successfully

Opening table 'marketplace_products'...
✓ Table opened successfully

⚙️  Creating scalar index on column 'id' with type 'BTREE'...
This may take several minutes for large datasets...

✓ Index created successfully!

================================================================================
✅ SUCCESS: Index has been created and is ready to use
================================================================================

You can now run your data upload operations.
```

### Timing

- Small tables (< 10k rows): ~30 seconds
- Medium tables (10k-100k rows): 2-5 minutes
- Large tables (> 100k rows): 5-15 minutes

### Troubleshooting

**Index already exists error**: This is OK! It means your table already has an index and is ready to use.

**Timeout error**: The script waits up to 15 minutes. If it times out, try running it again - the index creation may still complete in the background.

**Connection error**: Verify your `LANCEDB_API_KEY` is set correctly.

**Table not found**: Ensure the table exists and you have the correct permissions.

### Configuration

To use this script for a different table, modify the import at the top:

```python
# Change this line:
from src.services.chalk_demo.marketplace_product.webhook import Webhook as WebhookModel

# To import your webhook model:
from src.services.your_service.webhook import Webhook as WebhookModel
```

The script automatically reads all configuration from the `WebhookModel`:
- `lance_get_project_name()` - LanceDB project
- `lance_get_table_name()` - Table name
- `lance_get_primary_key()` - Column to index
- `lance_get_primary_key_index_type()` - Index type (BTREE, etc.)

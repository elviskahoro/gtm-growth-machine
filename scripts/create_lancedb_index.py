#!/usr/bin/env python3
"""Standalone script to create an index on a LanceDB table.

This script creates a scalar index on the primary key column to fix the
"number of un-indexed rows exceeds the maximum" error.

Usage:
    python scripts/create_lancedb_index.py

The script uses the configuration from the WebhookModel to connect to the
correct LanceDB project and table.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

# Add src directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.lance.setup import init_client  # noqa: E402
from src.services.chalk_demo.marketplace_product.webhook import (  # noqa: E402
    Webhook as WebhookModel,
)


def create_index() -> None:
    """Create scalar index on the primary key column."""
    # Get configuration from WebhookModel
    project_name: str = WebhookModel.lance_get_project_name()
    table_name: str = WebhookModel.lance_get_table_name()
    primary_key: str = WebhookModel.lance_get_primary_key()
    index_type: str = WebhookModel.lance_get_primary_key_index_type().upper()

    print("=" * 80)
    print("LanceDB Index Creation Script")
    print("=" * 80)
    print("\nConfiguration:")
    print(f"  Project:     {project_name}")
    print(f"  Table:       {table_name}")
    print(f"  Column:      {primary_key}")
    print(f"  Index Type:  {index_type}")
    print()

    # Check for API key
    if not os.getenv("LANCEDB_API_KEY"):
        print("❌ Error: LANCEDB_API_KEY environment variable not set")
        print("\nPlease set your LanceDB API key:")
        print("  export LANCEDB_API_KEY='your-api-key-here'")
        sys.exit(1)

    print("Connecting to LanceDB...")
    try:
        db = init_client(project_name=project_name)
        print("✓ Connected successfully")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        sys.exit(1)

    print(f"\nOpening table '{table_name}'...")
    try:
        table = db.open_table(name=table_name)
        print("✓ Table opened successfully")
    except Exception as e:
        print(f"❌ Failed to open table: {e}")
        print("\nPossible reasons:")
        print("  - Table does not exist")
        print("  - Incorrect table name")
        print("  - Insufficient permissions")
        sys.exit(1)

    print(
        f"\n⚙️  Creating scalar index on column '{primary_key}' with type '{index_type}'...",
    )
    print("This may take several minutes for large datasets...")
    print()

    try:
        table.create_scalar_index(
            column=primary_key,
            index_type=index_type,  # type: ignore[arg-type]
            replace=True,
            wait_timeout=timedelta(minutes=15),
        )
        print("✓ Index created successfully!")
        print()
        print("=" * 80)
        print("✅ SUCCESS: Index has been created and is ready to use")
        print("=" * 80)
        print("\nYou can now run your data upload operations.")

    except Exception as e:
        print(f"❌ Failed to create index: {e}")
        print()
        print("Possible reasons:")
        print("  - Index already exists (this is usually OK)")
        print("  - Column does not exist")
        print("  - Insufficient permissions")
        print("  - Timeout - try running the script again")
        print()
        print("If the error says the index already exists, your table is ready to use.")
        sys.exit(1)


if __name__ == "__main__":
    create_index()

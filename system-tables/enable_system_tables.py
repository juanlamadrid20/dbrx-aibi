import os
import requests
from typing import Dict, Any
import pprint


def get_headers() -> Dict[str, str]:
    """
    Fetch the authorization headers with the Databricks token.
    """
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    if not databricks_token:
        raise ValueError("Environment variable 'DATABRICKS_TOKEN' is not set.")
    return {"Authorization": f"Bearer {databricks_token}"}


def get_system_schemas(host: str, metastore_id: str) -> Any:
    """
    Retrieve system schemas for a given metastore ID.

    Args:
        host (str): The Databricks workspace host URL.
        metastore_id (str): The Unity Catalog metastore ID.

    Returns:
        Any: The JSON response containing the system schemas.
    """
    url = f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas"

    headers = get_headers()
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        response.raise_for_status()
    return response.json()


def enable_schema(host: str, metastore_id: str, schema_name: str) -> Any:
    """
    Enable a given schema for a specific metastore ID.

    Args:
        host (str): The Databricks workspace host URL.
        metastore_id (str): The Unity Catalog metastore ID.
        schema_name (str): The name of the system schema to enable.

    Returns:
        Any: The JSON response from the enable request or status code.
    """
    url = f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}"

    headers = get_headers()
    response = requests.put(url, headers=headers)
    if response.status_code not in (200, 204):
        response.raise_for_status()
    return response.status_code  # Generally, enabling returns a 204 status.


if __name__ == "__main__":
    # Define these variables
    HOST = "https://one-env-juan-lamadrid-sandbox.cloud.databricks.com"  # Replace this for other environments
    METASTORE_ID = "af1bc58c-8e79-477c-b186-f61e9405bd93"

    # List of schemas to enable
    SCHEMAS = [
        "compute",
        "marketplace",
        "query",
        "lakeflow",
        "lakeflow_preview",
        "serving",
        "mlflow",
        "storage",
        "lineage",
        "hms_to_uc_migration"
    ]

    try:
        # Fetch system schemas
        print("Fetching system schemas...")
        available_schemas = get_system_schemas(HOST, METASTORE_ID)
        print("Available system schemas:")
        pprint.pprint(available_schemas)

        # Iterate and enable each schema
        for schema_name in SCHEMAS:
            print(f"Enabling schema: {schema_name}...")
            try:
                status = enable_schema(HOST, METASTORE_ID, schema_name)
                print(f"Schema '{schema_name}' enabled successfully with status code: {status}")
            except Exception as e:
                print(f"Failed to enable schema '{schema_name}': {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

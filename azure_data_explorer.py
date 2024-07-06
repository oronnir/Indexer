from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, BlobDescriptor
from azure.identity import DefaultAzureCredential
from azure.kusto.data.data_format import DataFormat


class AzureDataExplorerClient:
    def __init__(self, cluster_url: str, database_name: str):
        # Initializes the credentials
        self.credential = DefaultAzureCredential()

        # Initializes the connection string builder for querying
        self.kcsb_query = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)
        self.kcsb_query.authority_id = "common"  # Replace 'common' with your Azure tenant ID if necessary
        self.query_client = KustoClient(self.kcsb_query)

        # Initializes the connection string builder for ingesting
        ingest_url = cluster_url.replace("https://", "https://ingest-")
        self.kcsb_ingest = KustoConnectionStringBuilder.with_aad_device_authentication(ingest_url)
        self.ingest_client = QueuedIngestClient(self.kcsb_ingest)

        self.database_name = database_name

    def execute_query(self, query: str):
        """Executes a KQL query against the ADX database."""
        try:
            response = self.query_client.execute(self.database_name, query)
            return AzureDataExplorerClient._parse_response(response)
        except KustoServiceError as e:
            print(f"Error executing query: {e}")
            return None

    @staticmethod
    def _parse_response(response):
        """Parses and returns the response from the ADX query."""
        result_table = response.primary_results[0]
        results = [row for row in result_table]
        return results

    def ingest_data_from_json(self, csv_file_path: str, table_name: str, mapping_name: str = None):
        """Ingests data from a JSON file into an ADX table."""
        ingestion_properties = IngestionProperties(
            database=self.database_name,
            table=table_name,
            data_format=DataFormat.JSON,
            ingestion_mapping_reference=mapping_name,
            # additional_properties={"ignoreFirstRecord": "true"}
        )

        try:
            blob_descriptor = BlobDescriptor(csv_file_path)
            self.ingest_client.ingest_from_blob(blob_descriptor, ingestion_properties)
            print("Ingestion request submitted.")
        except Exception as e:
            print(f"Error during ingestion: {e}")

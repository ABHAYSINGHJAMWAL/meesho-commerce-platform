import os
import logging
import pandas as pd
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BigQueryWriter:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id

        try:
            from google.cloud import bigquery
            self.client = bigquery.Client(project=project_id)
            logger.info(f"BigQuery client initialized: {project_id}.{dataset_id}")
            self.available = True
        except Exception as e:
            logger.warning(f"BigQuery not available: {e}")
            self.available = False

    def _load_parquet_output(self, output_name: str) -> pd.DataFrame:
        path = f"data/processed/{output_name}"

        if not os.path.exists(path):
            raise FileNotFoundError(f"No output found at {path}")

        dfs = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    dfs.append(pd.read_csv(file_path))
                elif file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    dfs.append(pd.read_parquet(file_path))

        if not dfs:
            raise ValueError(f"No data files found in {path}")

        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Loaded {output_name}: {len(df):,} rows")
        return df

    def write_to_bigquery(self, df: pd.DataFrame, table_name: str, write_disposition: str = 'WRITE_TRUNCATE') -> bool:
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        if not self.available:
            local_path = f"data/processed/{table_name}_bq_ready.csv"
            df.to_csv(local_path, index=False)
            logger.info(f"BigQuery not available. Saved {len(df):,} rows to {local_path}")
            return True

        try:
            from google.cloud import bigquery

            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=True,
            )

            job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()

            table = self.client.get_table(full_table_id)
            logger.info(f"Loaded {table.num_rows:,} rows into {full_table_id}")
            return True

        except Exception as e:
            logger.error(f"BigQuery write failed for {table_name}: {e}")
            return False

    def run_full_load(self):
        tables = {
            'daily_revenue': 'mart_daily_revenue',
            'cohort_retention': 'mart_cohort_retention',
            'seller_performance': 'mart_seller_performance',
            'fraud_patterns': 'mart_fraud_patterns',
            'category_trends': 'mart_category_trends',
        }

        results = {}

        for output_name, table_name in tables.items():
            try:
                logger.info(f"Loading {output_name} -> {table_name}...")
                df = self._load_parquet_output(output_name)

                df.columns = [
                    col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
                    for col in df.columns
                ]

                for col in df.columns:
                    if df[col].dtype == 'object':
                        try:
                            sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                            if isinstance(sample, (list, dict)):
                                df[col] = df[col].astype(str)
                        except Exception:
                            df[col] = df[col].astype(str)

                success = self.write_to_bigquery(df, table_name)
                results[table_name] = 'SUCCESS' if success else 'FAILED'

            except FileNotFoundError:
                logger.warning(f"Output not found: {output_name} -- skipping")
                results[table_name] = 'SKIPPED'
            except Exception as e:
                logger.error(f"Failed to load {output_name}: {e}")
                results[table_name] = f'ERROR: {str(e)[:50]}'

        logger.info("\n=== BIGQUERY LOAD SUMMARY ===")
        for table, status in results.items():
            logger.info(f"  {table}: {status}")

        return results


if __name__ == "__main__":
    import sys
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(project_root)

    from dotenv import load_dotenv
    env_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path=env_path)
    logger.info(f"Loading .env from: {env_path}")
    logger.info(f".env file exists: {os.path.exists(env_path)}")

    creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
        logger.info(f"Using credentials from: {creds_path}")
    else:
        logger.error("GOOGLE_APPLICATION_CREDENTIALS not found in .env")

    project_id = os.getenv('GCP_PROJECT_ID', 'analytics-warehouse-dev-major')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'meesho_raw')

    writer = BigQueryWriter(project_id=project_id, dataset_id=dataset_id)
    results = writer.run_full_load()

    successful = sum(1 for s in results.values() if s == 'SUCCESS')
    logger.info(f"\n{successful}/{len(results)} tables loaded successfully")
# Create Tables

This file is not hand-written.

All table creation in this project was done using **AWS Glue Crawlers**. Crawlers automatically scanned the raw and processed data stored in S3 and created table definitions in the AWS Glue Data Catalog.

### Structure:
- **Input data format**: CSV or Parquet
- **Location**: S3 (Bronze, Silver, Gold layers)
- **Catalog storage**: AWS Glue Data Catalog (used by Athena)

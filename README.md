## Youtube ETL with Apache Kafka and Docker

This project is an implementation of an ETL (Extract, Transform, Load) pipeline using Apache Airflow, Docker, and the YouTube API. It extracts data from multiple YouTube channels, transforms it into a structured format, and loads it into an AWS S3 bucket.

### Project Overview

This project is based on [this tutorial](https://www.youtube.com/watch?v=q8q3OFFfY6c&t=1652s) with modifications:

- **Environment**: Docker with WSL2 backend is used instead of EC2 for local development.
- **API**: YouTube API is used instead of the Twitter API because the latter is no longer free.

### DAG Structure

- **extract_data**: Extracts data from specified YouTube channels using the YouTube API.
- **transform_data**: Transforms the extracted data into a structured format using Pandas.
- **load_data_to_s3**: Loads the transformed data into an AWS S3 bucket using Boto3.

### Additional Notes

- This project is intended for educational purposes and may require further customization for production use.
- Feel free to add other channel ids or modify the extract_data function to gather other data from the channels.

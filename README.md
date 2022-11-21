# dbt-dynamic-sourcing
This script shows a way to dynamically source different data sets without the need to bring explicit naming to the data sources.

# Problem:
Many Datasets are loaded/exchanged/updated to GCP making it necessary to update the "source.yml" file each time.

### Consequences:
1) Time lost.
2) Human input error.
3) Script re-adjustment.

# Solution 

1) Get dataset information through GCP information in run-time.
2) Adapt script with information from point 1 by leveraging dbt-jinja capabilities.
3) Use dynamic sourcing for each dataset found in GCP.

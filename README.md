# dbt-dynamic-sourcing
This script shows a way to dynamically source different data sets without the need to bring explicit naming to the data sources.

# Problem:
Many Datasets are loaded/exchanged/updated to GCP making it necessary to update the "source.yml" file each time.

Imagine the following:

<img src="images/db2.jpg" width="600" />

1) Every entity (grey) has their own Datasets (colorful sheets in the middle)
2) Every Dataset (colorful sheets) could have OR NOT the *STANDARD* tables
3) Each Table must contain the same columns (just for the simplicity)

### >>PLEASE OBSERVE that entity 2 does not have CRM Dataset as entity 1 has 
###  and the Sales Dataset does not have the same 3 tables as entity 1. <<



### Consequences:
1) Time lost.
2) Human input error.
3) Script re-adjustment.

# Solution 

1) Get dataset information through GCP information in run-time.
2) Adapt script with information from point 1 by leveraging dbt-jinja capabilities.
3) Use dynamic sourcing for each dataset found in GCP.

# Cons:
1) No lineage trace on dbt since is dynamically traced and not statically.

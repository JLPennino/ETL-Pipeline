# ETL-Pipeline
1. Extraction of different file formats from the source cloud to the newly created bucket within source cloud for analysis/data transformation to be completed on each data format. 
2. Data transformation functionality to create for a meaningful output for each data format before the transformed files were to then be loaded into the new source cloud bucket. 
3. Having the newly transformed files be loaded into the final Bigquery database via GCS for preview and further analysis. 
4. Applying a reconciliation process which in this case was comparing/matching the MD5 hash values of the transferred files from S3 - GCS in order to make sure no file was tampered with during the transfer into GCS. The reconciliation status and the hash values from both sides were then updated into the final loaded table within Bigquery.

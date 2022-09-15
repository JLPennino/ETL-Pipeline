from datetime import datetime
import boto3
import io
import os
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import base64 
#Importing all required packages for the program to function

run_id_value = f'{datetime.now().strftime("%Y%M%d%H%M%S")}'

#Establishing Connections to GCP @ AWS and checking initial connection to all resources.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/jamyelorenzo.TRN/GCP/ServiceKey_GoogleBigquery.json'
client = bigquery.Client()
s3 = boto3.client('s3')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/jamyelorenzo.TRN/GCP/ServiceKey_GoogleCloud.json'
response = s3.list_buckets()
status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
if status == 200:
    print(f"Successful connection response. Status Code - {status}")
else:
    print("Connection Failed, Please try again") 
        

#s3 = boto3.resource('s3') 
#s3 = boto3.client('s3')
bucket = s3.Bucket('rawdata') #Transforming file through AWS - AWS
prefix_objs = bucket.objects.filter(Prefix="")
gcs_storage_client = storage.Client()
gcs_bucket = gcs_storage_client.get_bucket('jamye_lorenzo')
for obj in prefix_objs:
    key = obj.key
    body = obj.get()['Body'].read().decode('utf-8')
    print("Path",  "s3://rawdata/"+ key)
    print(body)
    df = pd.DataFrame(pd.read_csv(f's3://rawdata/' + key))
    df['file_name'] = run_id_value + '/' +key.split(".")[0] + "_transformed.csv"
    df ['run_id'] = run_id_value
    df['aws_hash_value'] = 0
    df['gcs_hash_value'] = 0
    df['reconcile_status'] = 'In_Progress'
    df['last_updated_timestamp'] = f'{datetime.now()}'
    
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        response = s3.put_object(
        Bucket='transformedata', Key= run_id_value + "/" + key.split(".")[0] + "_transformed.csv", Body=csv_buffer.getvalue()
        )
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}") 
    s3_file_to_load = (s3.get_object(Bucket='transformedata', Key= run_id_value + "/" +key.split(".")[0] + "_transformed.csv")['Body']).read().decode('utf-8')
    
    blob = gcs_bucket.blob(run_id_value + '/' + key.split(".")[0] + '_transformed.csv')

    blob.upload_from_string(s3_file_to_load, content_type= 'csv') #Loading from AWS - GCS


table_id = "autonomous-mote-357010.jamyelorenzo_test1.employee_data"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("ROW", "INTEGER"),
        bigquery.SchemaField("SALARY", "STRING"),
        bigquery.SchemaField("EMPLOYEE", "STRING"),
        bigquery.SchemaField("E_ID", "INTEGER"),
        bigquery.SchemaField("file_name", "STRING"),
        bigquery.SchemaField("run_id", "INTEGER"),
        bigquery.SchemaField("aws_hash_value", "STRING"),
        bigquery.SchemaField("gcp_hash_value", "STRING"),
        bigquery.SchemaField("reconcile_status", "STRING"),
        bigquery.SchemaField("last_updated_timestamp", "STRING"),

    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)
uri = 'gs://jamye_lorenzo/' + run_id_value + '*' 

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  

load_job.result()  

destination_table = client.get_table(table_id)  
print("Loaded {} rows.".format(destination_table.num_rows)) #Loading from GCS - GCS Bigquery
s3_transformed = boto3.resource('s3')

my_bucket = s3_transformed.Bucket('transformedata')
aws_hash_dict = {} 
for my_bucket_object in my_bucket.objects.all().filter(Prefix = run_id_value):
    #my_bucket_object.key = str(my_bucket_object.key).replace(run_id_value + "/", "")
    print(my_bucket_object.key)
    aws_hash_dict[my_bucket_object.key] = my_bucket_object.e_tag.strip('"')
    print(aws_hash_dict) #Getting the MD5 hash value from AWS side and storing the values in dictionary 
blobs = gcs_bucket.list_blobs(prefix=run_id_value)  
gcp_hash_dict = {}  
for blob in blobs:
    #blob.name = str(blob.name).replace("CSV_Objects/", "")
    gcp_hash_dict[blob.name] = base64.b64decode(blob.md5_hash).hex()
    print(gcp_hash_dict) #Getting the MD5 hash value from GCP side and storing the values in a dictionary 

for filename_key in aws_hash_dict.keys():
    aws_hash_md5 = aws_hash_dict[filename_key]
    gcp_hash_md5 = gcp_hash_dict[filename_key]
    reconcile_status = ''
    if aws_hash_md5 == gcp_hash_md5:
        reconcile_status_value = 'Success'
        print('Success  ' + aws_hash_md5 + "  "+ gcp_hash_md5)
        bq_query = """UPDATE autonomous-mote-357010.jamyelorenzo_test1.employee_data 
	    SET aws_hash_value = '{}', gcp_hash_value = '{}', reconcile_status = '{}'  
	    WHERE file_name = '{}' AND run_id = {}""".format(aws_hash_md5, gcp_hash_md5, reconcile_status_value, filename_key, run_id_value)
        print("Printing Query" + bq_query)
        query = client.query(bq_query)
        print(query)
        print(client)
        results = query.result()
        print(results)
        print(client) #Verifying that the MD5 values match from within each dictionary then updating the bigquery table accordingly to signify reconciliation was successful
    else:
        reconcile_status_value = 'Fail'
        print('Fail  '+ aws_hash_md5 + "  "+ gcp_hash_md5)
        



    


  

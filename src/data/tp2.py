import boto3
import pandas as pd
import pyodbc
from io import BytesIO

# Connexion à MinIO
minio_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1',
    use_ssl=False
)

bucket_name = 'yellowbucket'  

# Récupération de la liste des objets Parquet dans le bucket
parquet_files = []

try:
    objects = minio_client.list_objects(Bucket=bucket_name)
    for obj in objects.get('Contents', []):
        if obj['Key'].lower().endswith('.parquet'):
            parquet_files.append(obj['Key'])

    print("Fichiers Parquet trouvés :", parquet_files)

    # Connexion à la base de données SQL Server
    conn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=LAPTOP-MMF5I8NQ;DATABASE=Atl_datamart;UID= ;PWD= '
    )


    for file_name in parquet_files:
        try:
           
            response = minio_client.get_object(Bucket=bucket_name, Key=file_name)
            parquet_data = response['Body'].read()

    
            df = pd.read_parquet(BytesIO(parquet_data))

            # Insertion des données dans une table SQL Server
            cursor = conn.cursor()

            for index, row in df.iterrows():
                
                cursor.execute('''
                    INSERT INTO Table_taxi (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, 
                               store_and_fwd_flag,
                               PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
                               congestion_surcharge, Airport_fee ) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                ''', row['VendorID'], row['tpep_pickup_datetime'], row['tpep_dropoff_datetime'], row['passenger_count'], row['trip_distance']
                 ,row['RatecodeID'], row['store_and_fwd_flag'], row['PULocationID'], row['DOLocationID'], row['payment_type'], row['fare_amount'],
                   row['extra'], 
                  row['mta_tax'], row['tip_amount'], row['tolls_amount'], row['improvement_surcharge'], row['total_amount'], 
                  row['congestion_surcharge'], row['Airport_fee'] )  

            conn.commit()
            print(f"Les données du fichier {file_name} ont été insérées dans la base de données")

           
            cursor.close()

        except Exception as e:
            print(f"Erreur lors du traitement du fichier {file_name} : {e}")

    conn.close()

except Exception as e:
    print(f"Erreur lors de la récupération des objets dans le bucket ou de la connexion à la base de données : {e}")

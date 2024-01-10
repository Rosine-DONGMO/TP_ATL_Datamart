from minio import Minio
import boto3
import os


# Chemin du dossier local contenant les fichiers Parquet
folder_path =r'C:\Users\Rosine DONGMO\Desktop\Tp1-archi-decisionnelle\dataset_yellow_taxi'

# Nom du bucket MinIO
bucket_name = 'yellowbucket'

# connexion à MinIO en local
minio_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',  
    aws_access_key_id='minioadmin',  
    aws_secret_access_key='minioadmin', 
    region_name='us-east-1', 
    use_ssl=False  
)

# Liste tous les fichiers Parquet dans le dossier local
parquet_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]

# Boucle de chaque fichier Parquet et le stocke dans MinIO
for file_name in parquet_files:
    file_path = os.path.join(folder_path, file_name)

    # Chargement des fichiers Parquet dans le bucket

    with open(file_path, 'rb') as data:
        minio_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=data
        )
    print(f'Fichier {file_name} stocké dans MinIO dans le bucket {bucket_name}')


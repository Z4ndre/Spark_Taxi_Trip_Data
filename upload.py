import pandas as pd
import subprocess
import pyarrow
from hdfs import InsecureClient
import os

conteiner_id = ''

def upload_to_hadoop(local_file, hadoop_path):
    docker_container_id = f'{conteiner_id}'
    temp_file_path_in_container = f'/tmp/{os.path.basename(local_file)}'
    copy_command = f'docker cp {local_file} {docker_container_id}:{temp_file_path_in_container}'
    
    process = subprocess.run(copy_command, shell=True, text=True, capture_output=True)
    if process.returncode == 0:
        print('Файл успешно скопирован в Docker контейнер.')
    else:
        print(f'Ошибка во время копирования файла в Docker контейнер: {process.stderr}')
        return 

    hdfs_put_command = f'docker exec -i {docker_container_id} bash -c "hdfs dfs -put {temp_file_path_in_container} {hadoop_path}"'
    
    process = subprocess.run(hdfs_put_command, shell=True, text=True, capture_output=True)
    if process.returncode == 0:
        print('файл успешно загружен.')
    else:
        print(f'Ошибка во время загрузки файла в HDFS: {process.stderr}')

chunk_size = 50000 # количество строк на которые будем разбивать наш файл

path_to_ur_dir_in_hadoop = '/azhakhanov.andrew'

path_to_file = {
    "1": r"",
    "3": r"",
    "4": r"",
    "5": r""
}

for file_path in path_to_file.values():
    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size, delimiter = ",")):
        chunk.drop_duplicates()
        chunk.to_parquet(f'data_part_{i}.parquet')
        upload_to_hadoop(f'data_part_{i}.parquet', f'{path_to_ur_dir_in_hadoop}')



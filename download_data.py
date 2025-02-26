import requests
from tqdm import tqdm 

datasets = {
    2019: "2upf-qytp",
    2020: "kxp8-n2sj",
    2021: "m6nq-qud6",
    2022: "t29m-gskq",
    2023: "2yzn-sicd"  
}

def download_with_progress(url: str, filename: str):
    """Загрузка файла с отображением прогресса"""
    try:
        with requests.get(url, stream=True, timeout=10) as response:
            response.raise_for_status()
            
            # Получаем общий размер файла из заголовка
            total_size = int(response.headers.get('content-length', 0))
            
            # Создаем прогресс-бар
            with tqdm(
                total=total_size, 
                unit='B', 
                unit_scale=True, 
                desc=f"Загрузка {filename}", 
                ncols=80
            ) as pbar:
                
                with open(filename, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        pbar.update(len(chunk))
                        
        print(f"\nФайл {filename} успешно загружен!")
        return True
    
    except Exception as e:
        print(f"\nОшибка при загрузке {filename}: {str(e)}")
        return False

for year, dataset_id in datasets.items():
    csv_url = f"https://data.cityofnewyork.us/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"
    filename = f"yellow_taxi_{year}.csv"
    download_with_progress(csv_url, filename)
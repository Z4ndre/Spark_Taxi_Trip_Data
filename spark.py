from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession \
            .builder \
            .appName("Taxi") \
            .getOrCreate()

'''
Spark - ленивый, если ему не сказать "покажи", он ничего не покажет, поэтому обязательно для вывода полученного результата
добавлять .show() , можно прописать в .show(n), где n - количество строк которые мы хотим увидеть
'''

print("Активные Spark сессии:", spark.sparkContext.uiWebUrl)

PATH = './2019_Yellow_Taxi_Trip_Data.csv'

df = spark.read.csv(PATH, sep=',', header=True)

#df.show(2, False, True) # Отображение 2-х строк в виде столбца.

#df.printSchema() # Отображение типа данных

# Посмотрим уникальные значения VendorID
df.select('VendorID')\
    .distinct()\
    #.show(5, truncate=False)

# Посчитаем сколько строк для каждого значения VendorID и отсортируем по убиванию
df.groupBy('VendorID')\
    .agg(F.count('*'))\
    .orderBy(F.col('VendorID').desc())\
    #.show()

# Отображение всех столбцов из нашего DF
# Костыль, но не сработало обычное отображение прописав df.columns
columns = df.columns
# print(columns)

# Преобразуем перед сохранения некоторые столбцы
final = df\
        .select(
            'VendorID', 
            F.col('tpep_pickup_datetime').cast('timestamp'), 
            F.col('tpep_dropoff_datetime').cast('timestamp'), 
            F.col('passenger_count').cast('int'), 
            F.col('trip_distance').cast('double'), 
            'RatecodeID', 
            'store_and_fwd_flag', 
            'PULocationID', 
            'DOLocationID', 
            'payment_type', 
            'fare_amount', 
            'extra', 
            'mta_tax', 
            'tip_amount', 
            'tolls_amount',
            'improvement_surcharge',
            F.col('total_amount').cast('double'), 
            'congestion_surcharge'
        )

# Убедимся в преобразовании
# final.printSchema()

# Выведем первые 3 строки в которых VendorID = 1
final\
    .select('payment_type','tip_amount')\
        .where(F.col('VendorID') == '1')\
        #.show(3, False, True)

first_payment_type = final\
    .select('*')\
    .where(F.col('payment_type') == '1')
    
# first_payment_type.show(2, False, True)

# Сохраним наш DF c партицированием по vendorID 

first_payment_type\
    .write\
    .partitionBy('passenger_count')\
    .format('csv')\
    .options(header=True, sep=';')\
    .csv('data/final_partitioned')


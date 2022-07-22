from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as f # импортируем функции
import os


spark = SparkSession.builder\
    .master('local[*]') \
    .appName('dz_05.02') \
    .getOrCreate()

df = spark.read.\
option('inferSchema', True).\
option('sep', ',').\
option('header', True).\
csv('Downloads/covid-data.csv')

windowSpec = Window()

# df.printSchema()
'''
Задание 1
Выберите 15 стран с наибольшим процентом переболевших на 31 марта
(в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)
Проверим какие локации маркируются префиксом OWID_
'''

df_1 = df.filter(
    (f.col('date') <= f.lit('2022-03-31')) &
    (~f.col('iso_code').contains('OWID')) |
    (f.col('iso_code').contains('KOS'))).select([
    'iso_code',
    'location',
    'total_cases',
    'population'])

res_1 = df_1.groupBy('iso_code', 'location').agg(
    (f.sum('total_cases')/ f.max('population')).alias('percent')
).orderBy(f.col(
    'percent').desc()).limit(15)

'''
Задание 2.
Top 10 стран с максимальным зафиксированным кол-вом новых случаев 
за последнюю неделю марта 2021 в отсортированном порядке по убыванию 
(в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)
'''

df_2 = df.filter(
    ((f.col('date') >= f.lit('2021-03-25')) & (f.col('date') <= f.lit('2021-03-31'))) &
    ((~f.col('iso_code').contains('OWID')) | (f.col('iso_code').contains('KOS')))
).select([
    'location',
    'new_cases',
    'date'])

res_2 = df_2.withColumn('row_number',f.row_number().over(
    windowSpec.partitionBy('location').orderBy(f.col('new_cases').desc())
)).filter(
    f.col('row_number') == f.lit('1')).select([
    'location',
    'new_cases',
    'date']).orderBy(f.col(
    'new_cases').desc()).limit(10)

'''
Задание 3
Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. 
(например: в россии вчера было 9150 , сегодня 8763, итог: -387) 
(в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)
'''

df_3 = df.filter(
    (f.col('date') >= f.lit('2021-03-25')) &
    (f.col('date') <= f.lit('2021-03-31')) &
    (f.col('iso_code') == f.lit('RUS'))
    ).select([
    'date',
    'new_cases'])

df_3 = df_3.withColumn(
    'prev_day_cases', f.lag(f.col('new_cases'),1).over(windowSpec.partitionBy(f.lit(0)).orderBy('date')))

res_3 = df_3.withColumn('delta', f.col('new_cases') - f.col('prev_day_cases'))

# Удаляем ранее созданные папки с результатами в случае перезапуска скрипта
path = f'{os.getcwd()}/dz_05.02/' # Определяем путь до папки Downloads в которуюю поместим папки с результатами
os.system(f'rm -rf {path}res_1 {path}res_2 {path}res_3')

# Сохраняем результаты в своих директориях

res_1.write.option('header', 'true').csv(f'{path}res_1')
res_2.write.option('header', 'true').csv(f'{path}res_2')
res_3.write.option('header', 'true').csv(f'{path}res_3')

spark.stop()

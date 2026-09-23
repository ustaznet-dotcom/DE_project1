from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import clickhouse_connect

FILE_PATH = "/opt/jobs/russian_houses.csv"

# Границы "реалистичного" года постройки. Проверь по выводу min/max в шаге 3
# и объясни выбор в README (критерий 4: данные не редактируем, только исключаем из расчёта).
YEAR_MIN = 1700
YEAR_MAX = 2026

spark = SparkSession.builder.appName("RussianHousesAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# ---------------------------------------------------------------
# 1. Чтение
# Файл в UTF-16. Построчное чтение Spark режет файл по байту 0x0A,
# что ломает UTF-16 (символ перевода строки там двухбайтный),
# поэтому нужен multiLine=true: файл разбирает CSV-парсер целиком.
# ---------------------------------------------------------------
print("\n--- 1. Чтение ---")
df_raw = (
    spark.read
    .option("header", "true")
    .option("encoding", "UTF-16")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(FILE_PATH)
)


# На случай BOM в имени первой колонки
df_raw = df_raw.toDF(*[c.replace("\ufeff", "").strip() for c in df_raw.columns])

print(f"Колонки ({len(df_raw.columns)}): {df_raw.columns}")
print(f"Всего строк: {df_raw.count()}")
df_raw.printSchema()

# ---------------------------------------------------------------
# 2. Проверка корректности чтения
# ---------------------------------------------------------------
print("\n--- 2. Проверка данных ---")
df = df_raw.dropna(how="all").cache()
print(f"Строк после удаления полностью пустых: {df.count()}")
print(f"Строк с пустым house_id: {df.filter(F.col('house_id').isNull()).count()}")
print(f"Уникальных house_id: {df.select('house_id').distinct().count()}")

bad_text = df.filter(
    F.col("region").contains("\u0000") | F.col("region").contains("\ufffd")
).count()
print(f"Строк с битой кодировкой в region: {bad_text}  (ожидаем 0)")

df.select("house_id", "region", "locality_name", "maintenance_year", "square") \
    .show(5, truncate=False)


# ---------------------------------------------------------------
# 3. Типы
# square приходит вида "2 661.10" (пробел как разделитель тысяч),
# его убираем только для приведения к числу.
# ---------------------------------------------------------------
print("\n--- 3. Приведение типов ---")
df_typed = (
    df
    .withColumn("house_id", F.trim("house_id").cast("long"))
    .withColumn("latitude", F.trim("latitude").cast("double"))
    .withColumn("longitude", F.trim("longitude").cast("double"))
    .withColumn("maintenance_year",
                F.trim("maintenance_year").cast("double").cast("int"))
    .withColumn("square",
                F.regexp_replace(F.trim("square"), r"[\s\u00a0]", "").cast("double"))
    .withColumn("population", F.trim("population").cast("int"))
)
df_typed.printSchema()

df_typed.select(
    F.min("maintenance_year").alias("year_min"),
    F.max("maintenance_year").alias("year_max"),
    F.sum(F.col("maintenance_year").isNull().cast("int")).alias("year_null"),
    F.min("square").alias("square_min"),
    F.max("square").alias("square_max"),
    F.sum(F.col("square").isNull().cast("int")).alias("square_null"),
).show(truncate=False)
# null in columns
df_typed.select(
    F.sum(F.col("house_id").isNull().cast("int")).alias("house_id_null"),
    F.sum(F.col("region").isNull().cast("int")).alias("region_null"),
    F.sum(F.col("locality_name").isNull().cast("int")).alias("locality_name_null"),
    F.sum(F.col("address").isNull().cast("int")).alias("address_null"),
).show(truncate=False)

# data types


df_years = df_typed.filter(
    F.col("maintenance_year").between(YEAR_MIN, YEAR_MAX)
)
print(f"Строк с годом в [{YEAR_MIN}, {YEAR_MAX}]: {df_years.count()}")
print("down")
df_typed.select(F.countDistinct("locality_name").alias("n_localities")).show()

print("down2")
print(df_typed.filter(~F.col("maintenance_year").between(1700, 2026)).count())
# ---------------------------------------------------------------
# 4. Средний и медианный год (медиана точная)
# ---------------------------------------------------------------
print("\n--- 4. Средний и медианный год постройки ---")
df_years.select(
    F.round(F.avg("maintenance_year"), 2).alias("avg_year"),
    F.median("maintenance_year").alias("median_year"),
).show()

# ---------------------------------------------------------------
# 5. Топ-10 областей и городов
# Город считаем вместе с областью: одно и то же название встречается в разных регионах.
# ---------------------------------------------------------------
print("\n--- 5a. Топ-10 областей ---")
(df_typed.groupBy("region").count()
    .orderBy(F.desc("count")).limit(10).show(truncate=False))

print("\n--- 5b. Топ-10 городов (область + населённый пункт) ---")
(df_typed.groupBy("region", "locality_name").count()
    .orderBy(F.desc("count")).limit(10).show(truncate=False))

# ---------------------------------------------------------------
# 6. Мин/макс площадь в каждой области (по одному зданию;
# при равенстве берём меньший house_id). Нулевую площадь исключаем.
# ---------------------------------------------------------------
print("\n--- 6. Здания с макс./мин. площадью по областям ---")
df_area = df_typed.filter(F.col("square") > 0)

w_max = Window.partitionBy("region").orderBy(F.desc("square"), "house_id")
w_min = Window.partitionBy("region").orderBy(F.asc("square"), "house_id")

cols = ["region", "house_id", "address", "square"]
df_max = (df_area.withColumn("rn", F.row_number().over(w_max))
          .filter("rn = 1").select(*cols).withColumn("kind", F.lit("max")))
df_min = (df_area.withColumn("rn", F.row_number().over(w_min))
          .filter("rn = 1").select(*cols).withColumn("kind", F.lit("min")))

df_extremes = df_max.unionByName(df_min).orderBy("region", "kind")
df_extremes.show(20, truncate=False)

# ---------------------------------------------------------------
# 7. Десятилетия
# ---------------------------------------------------------------
print("\n--- 7. Здания по десятилетиям ---")
(df_years
    .withColumn("decade", (F.floor(F.col("maintenance_year") / 10) * 10).cast("int"))
    .groupBy("decade").count()
    .orderBy("decade")
    .show(100, truncate=False))
pandas_df = df_typed.select("house_id", "address", "region", "locality_name", "maintenance_year", "square").toPandas()
client = clickhouse_connect.get_client(host="clickhouse", port=8123, username="dwh_user", password="dwh_password")
client.command("TRUNCATE TABLE default.buildings")
client.insert_df(table="buildings", df=pandas_df)
top_25_square_hom = client.query("""
SELECT
    address,
    square
FROM buildings
WHERE (square > 60) AND (square < 1000000)
ORDER BY square DESC
LIMIT 25
""")
for row in top_25_square_hom.result_rows:
    print(row)

spark.stop()
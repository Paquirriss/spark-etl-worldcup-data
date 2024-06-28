# 🏆 Spark WorldCup ETL

ETL implementado en Apache Spark, diseñado para la extracción, transformación y carga de datos relacionados con la Copa del Mundo
## ⚽ Propósito

Quería aprender a usar Apache Spark y como tengo una pasión por el futbol, quize aprovechar para juntar estos 2 dos gusto``s que tengo por la ingenieria de datos y el futbol.

Entonces a partir de un CSV de los datos de la copa del mundo hice un ETL para poder crear un modelo dimensional que nos ayudara a analizar datos del mundial más especifico en los goles y las tarjetas rojas



## 🖱️ Tecnologías usadas
[![Apache Spark](https://a11ybadges.com/badge?logo=apachespark)](https://spark.apache.org/)
[![Power BI](https://a11ybadges.com/badge?logo=powerbi)](https://www.microsoft.com/es-es/power-platform/products/power-bi)
[![Python](https://a11ybadges.com/badge?logo=python)](https://www.python.org/)
[![Google Colab](https://a11ybadges.com/badge?logo=googlecolab)](https://colab.research.google.com/)


## 💻 Uso/Ejemplos

#### Adjunto el link del csv
[Football - FIFA World Cup, 1930 - 2022](https://www.kaggle.com/datasets/piterfm/fifa-football-world-cup?select=matches_1930_2022.csv)


#### En colab ya está el entorno para ejecutar apache Spark entonces sólo deberíamos instalarlo
```python
pip install pyspark
```
#### Aquí las librerías que uso para el ETL e inicio el spark sesión
```python
spark = SparkSession.builder \
    .appName("ETL FIFA mundial") \
    .config("spark.sql.parquet.outputEncoding", "UTF-8") \
    .getOrCreate()
```
#### Importante en al extracción usar UTF-8 y pasar a parquet para una mejor rendimiento
```python
csv_file_path = "/content/dataset.csv"

df = spark.read.csv(csv_file_path, header=True, inferSchema=True, encoding="UTF-8")

parquet_file = "/content/Fifas.parquet"

df_clean.write.parquet(parquet_file)

```

#### Para la transformación describo las funciones más importantes

#### Para asignar un id
```python
dim_referees = dim_referees.withColumn("id", monotonically_increasing_id() + 1)
```

#### Para pasar a dim date selecciono primero la columna y cuantos valores depues de esa columna escojo
```python
dim_matches = df_parquet.withColumn("Date",
    concat(
        substring(col("Date"), 1, 4),  # Año
        substring(col("Date"), 6, 2),  # Mes
        substring(col("Date"), 9, 2)   # Día
    )
)
```

#### Para eliminar valores especifico la columna que es lo que eliminaré (en esta caso (P) y digo con que lo rempazaré "")
```python
dim_players = dim_players.withColumn("player", regexp_replace(col("player"), r"\(P\)", ""))
```

#### En la carga como no tengo ya creditos en ningún servicio en la nube tuve que pasarlos como csv 
```python
dim_referees_pandas = dim_referees.toPandas()
dim_referees_pandas.to_csv('dim_referees.csv', index=False, encoding='utf-8'
```
En caso que quieran subirlo a una base de datos, les adjunto la documentación
[pandas.DataFrame.to_sql](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html)

Para la dimensión de tiempo utilicé este código
[dim_date](https://gist.github.com/sunnycmf/131a10a17d226e2ffb69)

## 📊 Modelo dimensional

### Tablas del Modelo Dimensional
El modelo está compuesto por varias tablas de dimensiones (dim) y tablas de hechos (fact).
### Dimensiones
+ dim_players
+ dim_matches
+ dim_referees
+ dim_teams 
+ dim_date 

### Hechos
+ f_goals
+ f_red_cards



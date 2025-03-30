from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Criando a sessão Spark
spark = SparkSession.builder.appName("ETL Big Data").getOrCreate()

# Carregando os datasets
df_liquor = spark.read.csv("iowa_liquor_sales.csv", header=True, inferSchema=True)
df_f1 = spark.read.csv("f1_champions.csv", header=True, inferSchema=True)

# Transformação - Selecionando e renomeando colunas do dataset de bebidas
df_liquor = df_liquor.select(
    col("Store Number").alias("ID_Loja"),
    col("Item Number").alias("ID_Produto"),
    col("Date").alias("Data_Venda"),
    col("Bottles Sold").alias("Volume_Vendido"),
    col("Sale (Dollars)").alias("Receita_Total")
)

# Transformação - Selecionando e renomeando colunas do dataset de F1
df_f1 = df_f1.select(
    col("raceId").alias("ID_Corrida"),
    col("driverId").alias("ID_Piloto"),
    col("constructorId").alias("ID_Equipe"),
    col("statusId").alias("ID_Status"),
    col("points").alias("Pontos")
)

# Agregação - Calculando total de vendas por loja
df_liquor_agg = df_liquor.groupBy("ID_Loja").agg(sum("Receita_Total").alias("Receita_Total_Loja"))

# Salvando os dados transformados
df_liquor.write.mode("overwrite").parquet("processed_liquor_sales.parquet")
df_f1.write.mode("overwrite").parquet("processed_f1_results.parquet")

# Finalizando a sessão
spark.stop()

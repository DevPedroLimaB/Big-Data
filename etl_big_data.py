from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Criando a sessão Spark
spark = SparkSession.builder.appName("ETL Big Data").getOrCreate()

# Definição do esquema para o dataset de bebidas
df_liquor_schema = StructType([
    StructField("Store Number", IntegerType(), True),
    StructField("Item Number", IntegerType(), True),
    StructField("Date", StringType(), True),  # Será convertido posteriormente para DateType
    StructField("Bottles Sold", IntegerType(), True),
    StructField("Sale (Dollars)", DoubleType(), True)
])

# Definição do esquema para o dataset de Fórmula 1
df_f1_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("statusId", IntegerType(), True),
    StructField("points", DoubleType(), True)
])

# Carregando os datasets com esquema explícito
df_liquor = spark.read.csv("iowa_liquor_sales.csv", schema=df_liquor_schema, header=True)
df_f1 = spark.read.csv("f1_champions.csv", schema=df_f1_schema, header=True)

# Transformação - Selecionando e renomeando colunas do dataset de bebidas
df_liquor = df_liquor.select(
    col("Store Number").alias("ID_Loja"),
    col("Item Number").alias("ID_Produto"),
    to_date(col("Date"), "MM/dd/yyyy").alias("Data_Venda"),
    col("Bottles Sold").alias("Volume_Vendido"),
    col("Sale (Dollars)").alias("Receita_Total")
)

# Criando coluna de Ano para particionamento
df_liquor = df_liquor.withColumn("Ano", col("Data_Venda").substr(1, 4))

# Transformação - Selecionando e renomeando colunas do dataset de F1
df_f1 = df_f1.select(
    col("raceId").alias("ID_Corrida"),
    col("driverId").alias("ID_Piloto"),
    col("constructorId").alias("ID_Equipe"),
    col("statusId").alias("ID_Status"),
    col("points").alias("Pontos")
)

# Agregação - Calculando total de vendas por loja com reparticionamento para melhor performance
df_liquor_agg = df_liquor.repartition("ID_Loja").groupBy("ID_Loja").agg(
    sum("Receita_Total").alias("Receita_Total_Loja")
)

# Logs para depuração
print("Iniciando processamento...")
print(f"Total de registros no dataset de bebidas: {df_liquor.count()}")
print(f"Total de registros no dataset de F1: {df_f1.count()}")
print("Transformação concluída. Salvando arquivos...")

# Salvando os dados transformados em formato Parquet com particionamento
df_liquor.write.partitionBy("Ano").mode("overwrite").parquet("processed_liquor_sales.parquet")
df_f1.write.mode("overwrite").parquet("processed_f1_results.parquet")

# Finalizando a sessão
spark.stop()

print("Processamento concluído com sucesso!")

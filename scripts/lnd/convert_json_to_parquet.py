import pyspark.sql.functions as F
from pyspark.sql.types import StructType,ArrayType
from pyspark.sql import DataFrame
df = spark.read.option("multiline", "true").json("storage/lnd/sidra_ibge_2025-08-28_20-13-23.json")

df_flat = df.select(
    F.col("Classificacoes"),
    F.col("DataAtualizacao"),
    F.col("Fonte"),
    F.col("Id"),
    F.col("Nome"),
    F.col("Notas"),
    F.col("UnidadesDeMedida"),
    F.col("Variaveis"),

    F.col("Periodos.Conjuntos").alias("Periodos_Conjuntos"),
    F.col("Periodos.Nome").alias("Periodos_Nome"),
    F.col("Periodos.Periodicidade").alias("Periodos_Periodicidade"),
    F.col("Periodos.Periodos").alias("Periodos_Periodos"),

    F.col("Pesquisa.Id").alias("Pesquisa_Id"),
    F.col("Pesquisa.Nome").alias("Pesquisa_Nome"),
    F.col("Pesquisa.Temas").alias("Pesquisa_Temas"),
    F.col("Pesquisa.UrlSidra").alias("Pesquisa_UrlSidra"),

    
    F.col("Territorios.DicionarioNiveis").alias("Territorios_DicionarioNiveis"),
    F.col("Territorios.DicionarioUnidades").alias("Territorios_DicionarioUnidades"),
    F.col("Territorios.NiveisTabela").alias("Territorios_NiveisTabela"),
    F.col("Territorios.Nome").alias("Territorios_Nome"),
    F.col("Territorios.VisoesTerritoriais").alias("Territorios_VisoesTerritoriais")
)


df_exploded = df_flat


df_exploded = df_exploded.withColumn("Classificacoes", F.explode_outer("Classificacoes"))
df_exploded = df_exploded.withColumn("Notas", F.explode_outer("Notas"))
df_exploded = df_exploded.withColumn("Periodos_Conjuntos", F.explode_outer("Periodos_Conjuntos"))
df_exploded = df_exploded.withColumn("Periodos_Periodos", F.explode_outer("Periodos_Periodos"))
df_exploded = df_exploded.withColumn("Pesquisa_Temas", F.explode_outer("Pesquisa_Temas"))
df_exploded = df_exploded.withColumn("Territorios_NiveisTabela", F.explode_outer("Territorios_NiveisTabela"))
df_exploded = df_exploded.withColumn("Territorios_VisoesTerritoriais", F.explode_outer("Territorios_VisoesTerritoriais"))
df_exploded = df_exploded.withColumn("UnidadesDeMedida", F.explode_outer("UnidadesDeMedida"))
df_exploded = df_exploded.withColumn("Variaveis", F.explode_outer("Variaveis"))


df_exploded = df_exploded.select(
    "Classificacoes",
    "DataAtualizacao",
    "Fonte",
    "Id",
    "Nome",
    "Notas",
    "Periodos_Conjuntos",
    F.col("Periodos_Nome").alias("Periodos_Nome"),
    F.col("Periodos_Periodicidade").alias("Periodos_Periodicidade"),
    "Periodos_Periodos",
    "Pesquisa_Id",
    "Pesquisa_Nome",
    "Pesquisa_Temas",
    "Pesquisa_UrlSidra",
    "Territorios_DicionarioNiveis",
    "Territorios_DicionarioUnidades",
    "Territorios_NiveisTabela",
    "Territorios_Nome",
    "Territorios_VisoesTerritoriais",
    "UnidadesDeMedida",
    "Variaveis"
)
df_exploded.write.mode("overwrite").parquet("storage/raw")
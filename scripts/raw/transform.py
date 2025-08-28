import pyspark.sql.functions as F
df = spark.read.format("parquet").load("storage/raw")

#este método retorna todos dos meses dentro de cada periodo indentificado no arquivo dos metadados
def get_months_in_each_duration(df):
    df_periodos = df.select("Periodos_Periodos.*").dropDuplicates()
    df_conjuntos = df.select("Periodos_Conjuntos.*").withColumn("Id_meses",F.explode("Periodos")).dropDuplicates()
    df_conjuntos = df_conjuntos.withColumn("Id_meses", F.col("Id_meses").cast("int"))
    df_conjuntos = df_conjuntos.withColumnRenamed("Id","Id_conjuntos")
    df_conjuntos = df_conjuntos.withColumnRenamed("Nome","Nome_conjuntos")
    df_periodos = df_periodos.withColumn("Id", F.col("Id").cast("int"))
    df_result = df_conjuntos.join(df_periodos, df_conjuntos.Id_meses == df_periodos.Id, "inner").drop("Periodos")
    return df_result
#retorna a quantidade de meses em cada periodo e qual o mes mais recente da série
def get_number_of_months_in_each_period(df):
    
    mes_map = {
        "janeiro": "01",
        "fevereiro": "02",
        "março": "03",
        "abril": "04",
        "maio": "05",
        "junho": "06",
        "julho": "07",
        "agosto": "08",
        "setembro": "09",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12"
    }
    for mes in mes_map:
        df = df.withColumn("Nome",F.regexp_replace("Nome",mes, mes_map[mes]))
    df = df.withColumn("mes", F.regexp_replace(F.col("nome"), " ", "-"))
    
    df = df.withColumn("mes_data", F.to_date(F.concat(F.lit("01-"), "mes"), "dd-MM-yyyy"))
    
    periodo = df.groupBy("Id_conjuntos").agg(
        F.min("mes_data").alias("inicio"),
        F.max("mes_data").alias("fim"),
        F.count("Id_conjuntos")
    ).withColumnRenamed("count(Id_conjuntos)","Meses no periodo")
    
    return periodo
#faz o flatten dos dados de uma columna do dataset
def get_variabeis(df):
    return df.select("Variaveis.*").dropDuplicates()

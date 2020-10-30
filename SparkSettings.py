from pyspark.sql import SparkSession

spark = SparkSession.\
    builder.\
    appName('SPARK - Teste técnico Cognitivio.ai - Engenheiro de dados').\
    getOrCreate()
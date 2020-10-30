import json
from pyspark.sql import functions as F
from SparkSettings import spark


class DataPreparation:

    def __init__(self, csv_file_path, json_schema_path, output_path, csv_header=True, csv_infer=True):
        self.csv_data_frame = spark.read.csv(csv_file_path, header=csv_header, inferSchema=csv_infer)
        self.output_path = output_path

        try:
            with open(json_schema_path, 'r') as f:
                self.file_schema = json.load(f)
        except FileNotFoundError:
            print(f'Arquivo {json_schema_path} não encontrado')

    @staticmethod
    def casting_fields(data_frame: spark, schema_casting: dict) -> spark.createDataFrame:
        """
        Funcao para fazer casting dos campos dos dataframes a partir de um schema em json
        :param data_frame: data frame de entrada com os dados para conversao
        :param schema_casting: objeto json com o schema a ser utilizado
        :return: novo data frame com os campos convertidos
        """

        fields = data_frame.dtypes
        return data_frame.select([
            data_frame[field].cast(schema_casting.get(field, datatype)).alias(field) for field, datatype in fields
        ])

    @staticmethod
    def dedup_records(data_frame: spark, field_id: str, field_current: str) -> spark.createDataFrame:
        """
        Funcao para deduplicar os registros retornando sempre o mais recente
        :param data_frame: data frame de entrada com os registros a serem deduplicados
        :param field_id: nome do campo identificador do registro
        :param field_current: nome do campo que identifica o registro mais recente
        :return: novo data frame com os registros deduplicados
        """

        field_max = f'max_{field_current}'
        df_dedup = data_frame.groupBy(field_id).agg(F.max(field_current).alias(field_max))
        df_dedup.createTempView("tb_dedup")
        data_frame.createTempView("tb_full")
        return spark.sql(f"""select f.* from tb_full f
                         join tb_dedup d on
                            f.{field_id} = d.{field_id} and
                            f.{field_current} = d.{field_max}
                        order by 1""")

    @staticmethod
    def write_parquet_file(data_frame: spark, path_output: str) -> spark.createDataFrame:
        """

        :param data_frame:
        :param path_output:
        :return:
        """
        data_frame.write.mode('overwrite').parquet(path_output)

    def main(self):
        print("Iniciando processo de preparacao dos dados (ETL). . .")
        print("Convertendo tipos de dados dos campos. . .")
        df = self.casting_fields(self.csv_data_frame, self.file_schema)
        print("Deduplicando registros. . .")
        df2 = self.dedup_records(df, 'id', 'update_date')
        print("Salvando arquivo Parquet. . .")
        self.write_parquet_file(df2, self.output_path)
        print("Processo finalizado")


if __name__ == '__main__':
    dp = DataPreparation(csv_file_path='data/input/users/load.csv',
                         json_schema_path='config/types_mapping.json',
                         output_path='data/output/')
    dp.main()

"""
GenericHelper.py
~~~~~~~~

Module containing helper function for generic utility funcs
"""
from os import environ, listdir, path
import json


class GenericHelper(object):

    @staticmethod
    def read_tsv_file_load_as_df(spark, data_file_path, schema, logger):
        data_frame = spark.read.csv(data_file_path, sep=r'\t', header=True, schema=schema)
        return data_frame

    @staticmethod
    def read_tsv_file_load_as_df_v1(spark, data_file_path, logger):
        data_frame = spark.read.csv(data_file_path, sep=r'\t', header=True, inferSchema=True)
        return data_frame

    @staticmethod
    def load_configs_from_local(logger):
        conf_files_dir = "../configs/"
        config_files = [filename
                        for filename in listdir(conf_files_dir)
                        if filename.endswith('config.json')]

        if config_files:
            path_to_config_file = path.join(conf_files_dir, config_files[0])
            with open(path_to_config_file, 'r') as config_file:
                config_dict = json.load(config_file)
            logger.warn('GenericHelper :loaded config from ' + config_files[0])
        else:
            logger.warn('GenericHelper: no config file found')
            config_dict = None

        return config_dict

    @staticmethod
    def write_data_frame_as_parquet(data_frame, s3_path, mode):
        data_frame.write.parquet(s3_path, mode=mode)

    @staticmethod
    def write_data_frame_to_postgres(data_frame, config, table_name, mode):
        url = "jdbc:postgresql://" + config['db_host'] + ":" + config['db_port'] + "/" + config['db_schema']
        properties = {"user": config['db_user'], "password": config['db_pass'], "driver": "org.postgresql.Driver"}
        data_frame.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)



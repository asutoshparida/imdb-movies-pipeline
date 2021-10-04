"""
Spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

from os import environ, listdir, path
import json

from botocore.exceptions import ParamValidationError
from pyspark.sql import SparkSession
import os
from dependencies import Logging
from helper.S3Helper import S3Helper as helper


class Spark(object):
    """Wrapper class for Apache Spark.
    """

    @staticmethod
    def start_spark(app_name='my_spark_app', master='local[*]', aws_access_id='', aws_secret_key='',
                    config_file_path='', jar_packages=[],
                    spark_config={}, environment='DEV'):
        """Start Spark session, get Spark logger and load config files.

        :param app_name: Name of Spark app.
        :param master: Cluster connection details (defaults to local[*]).
        :param jar_packages: List of Spark JAR package names.
        :param aws_access_id:
        :param aws_secret_key:
        :param config_file_path:
        :param spark_config: Dictionary of config key-value pairs.
        :param environment: DEV/PROD env.
        :return: A tuple of references to the Spark session, logger and
            config dict (only if available).
        """

        # detect execution environment
        flag_debug = 'DEBUG' in environ.keys()

        spark_jars_packages = ','.join(list(jar_packages))
        submit_args = "--packages {0} pyspark-shell".format(spark_jars_packages)
        os.environ["PYSPARK_SUBMIT_ARGS"] = submit_args

        if not flag_debug or environment.upper() == 'PROD':
            # get Spark session factory
            spark_builder = (
                SparkSession
                    .builder
                    .enableHiveSupport()
                    .appName(app_name))
        else:
            # get Spark session factory
            spark_builder = (
                SparkSession
                    .builder
                    .master(master)
                    .enableHiveSupport()
                    .appName(app_name))

        # create Spark JAR packages string
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        if environment.upper() == 'DEV':
            spark_builder.config("spark.sql.session.timeZone", "America/New_York")
        elif environment.upper() == 'PROD':
            spark_builder.config("spark.sql.session.timeZone", "America/Los_Angeles")

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

        # create session and retrieve Spark logger object
        spark_sess = spark_builder.getOrCreate()
        spark_logger = Logging.Log4j(spark_sess)

        # get config file if sent to cluster with --files
        config_dict = None
        try:
            if config_file_path:
                bucket, key = helper.split_s3_path(config_file_path)
                config_file_content = helper.read_s3_file(bucket, key, aws_access_id, aws_secret_key)
                config_dict = json.loads(config_file_content)
                spark_logger.warn(' Spark: loaded config from ' + config_file_path)
            else:
                spark_logger.warn(' Spark: no config file found')
                config_dict = None
        except ParamValidationError:
            spark_logger.warn(" Spark: An exception occurred while reading config file")

        return spark_sess, spark_logger, config_dict

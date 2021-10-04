import sys
from dependencies.Spark import Spark
from service.IMDBMoviesService import IMDBMoviesService as service
from helper.GenericHelper import GenericHelper as gh


def main():
    """Main ETL script definition.
    :return: None
    """
    args = sys.argv[1:]
    aws_access_id = args[0]
    aws_secret_key = args[1]
    config_s3_path = args[2]
    work_env = args[3]
    '''     
    start Spark application and get Spark session, logger and config
    '''

    spark, log, config = Spark.start_spark(
        app_name='imdb_movies_pipeline', aws_access_id=aws_access_id, aws_secret_key=aws_secret_key,
        config_file_path=config_s3_path,
        jar_packages=['org.apache.hadoop:hadoop-aws:3.1.2', 'org.apache.hadoop:hadoop-common:3.1.2',
                      'org.apache.hadoop:hadoop-client:3.1.2', 'org.postgresql:postgresql:42.2.16',
                      'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2']
        , environment=work_env)

    # If no proper S3 config file path not given as
    # parameter read configs from local ../configs folder
    if config is None:
        config = gh.load_configs_from_local(log)

    '''
    Configure S3 credentials
    '''
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3a.access.key", config['awsAccessKeyId'])
    hadoop_conf.set("fs.s3a.secret.key", config['awsSecretAccessKey'])

    # log that main ETL job is starting
    log.info(' IMDBMoviesETL: imdb_movies_pipeline is up-and-running')

    service.process_imdb_movies(spark=spark, logger=log, config=config)

    # log the success and terminate Spark application
    log.info(' IMDBMoviesETL: imdb_movies_pipeline is finished')
    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

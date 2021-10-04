"""
IMDBMoviesService.py
~~~~~~~~

Module containing service function for IMDB Data
"""
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from helper.GenericHelper import GenericHelper as gh


class IMDBMoviesService(object):
    """Wrapper service class for IMDB Movies.
    """

    title_akas_schema = StructType([ \
        StructField("titleId", StringType(), False), \
        StructField("ordering", IntegerType(), True), \
        StructField("title", StringType(), True), \
        StructField("region", StringType(), True), \
        StructField("language", StringType(), True), \
        StructField("types", IntegerType(), True), \
        StructField("attributes", StringType(), False), \
        StructField("isOriginalTitle", IntegerType(), True) \
        ])

    title_basic_schema = StructType([ \
        StructField("tconst", StringType(), False), \
        StructField("titleType", StringType(), True), \
        StructField("primaryTitle", StringType(), True), \
        StructField("originalTitle", StringType(), True), \
        StructField("isAdult", IntegerType(), True), \
        StructField("startYear", IntegerType(), True), \
        StructField("endYear", IntegerType(), False), \
        StructField("runtimeMinutes", IntegerType(), True), \
        StructField("genres", StringType(), True) \
        ])

    @classmethod
    def process_imdb_movies(cls, spark, logger, config):

        title_akas_data_frame = cls.import_imdb_akas_data(spark, logger, config)

        title_basics_data_frame = cls.import_imdb_basic_data(spark, logger, config)

        '''Inner join both akas and basics datasets to get the combined dataset'''
        final_data_frame = cls.join_imdb_basic_akas_data(title_akas_data_frame, title_basics_data_frame, logger)

        '''write the combined dataset to S3 as parquet file in Append Mode'''
        try:
            gh.write_data_frame_as_parquet(final_data_frame, config['s3DataLakeLocation'], "append")
            logger.info(" IMDBMoviesService: joined data set appended to S3 data lake.")
        except Py4JJavaError as er:
            logger.error(
                " IMDBMoviesService: An exception occurred while writing to s3DataLakeLocation" + str(er.java_exception))

        '''write the combined dataset to postgres in overwrite Mode'''
        gh.write_data_frame_to_postgres(final_data_frame, config, "imdb_movies", "overwrite")


    @classmethod
    def import_imdb_akas_data(cls, spark, logger, config):
        '''
        reading title.akas.tsv from S3 or local path (here for this assessment We are reading from local path) and
        repartition to 200
        '''
        tmp_title_akas_data_frame = gh.read_tsv_file_load_as_df(spark, config['titleAkasPath'],
                                                                cls.title_akas_schema, logger)
        title_akas_data_frame = tmp_title_akas_data_frame.repartition(200)
        logger.info(" IMDBMoviesService: title.akas.tsv data set DAG")

        return title_akas_data_frame

    @classmethod
    def import_imdb_basic_data(cls, spark, logger, config):
        '''
        reading title.basics.tsv from S3 or local path (here for this assessment We are reading from local path)
        and repartition to 200 '''
        tmp_title_basics_data_frame = gh.read_tsv_file_load_as_df(spark, config['titleBasicsPath'],
                                                                  cls.title_basic_schema, logger)
        title_basics_data_frame = tmp_title_basics_data_frame.repartition(200)
        logger.info(" IMDBMoviesService: title.basics.tsv data set DAG")

        return title_basics_data_frame

    @staticmethod
    def join_imdb_basic_akas_data(akas_data_frame, basics_data_frame, logger):

        final_data_frame = akas_data_frame.join(basics_data_frame, akas_data_frame.titleId
                                                == basics_data_frame.tconst, "inner").drop(col("tconst"))
        logger.info(" IMDBMoviesService: joined data set DAG")

        return final_data_frame


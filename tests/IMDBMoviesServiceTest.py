"""
IMDBMoviesServiceTest.py
~~~~~~~~~~~~~~~
This module contains integration tests for the service class.
"""
import unittest
from dependencies.Spark import Spark
from service.IMDBMoviesService import IMDBMoviesService as service
from helper.GenericHelper import GenericHelper as gh


class IMDBMoviesServiceTest(unittest.TestCase):
    """Integraion test for IMDBMoviesService.py
       Here using different logger from spark logger as these are rest cases
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        work_env = "DEV"
        self.spark, self.logger, self.config = Spark.start_spark(app_name='imdb_movies_test', aws_access_id='',
                                                              aws_secret_key='',
                                                              config_file_path='',
                                                              jar_packages=['org.apache.hadoop:hadoop-aws:3.1.2',
                                                                            'org.apache.hadoop:hadoop-common:3.1.2',
                                                                            'org.apache.hadoop:hadoop-client:3.1.2',
                                                                            'org.postgresql:postgresql:42.2.16'],
                                                              environment=work_env)
        if self.config is None:
            self.config = gh.load_configs_from_local(self.logger)

        self.config['titleAkasPath'] = "../tests/data/title.akas.tsv"
        self.config['titleBasicsPath'] = "../tests/data/title.basics.tsv"

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_import_imdb_akas_data(self):
        """Test import_imdb_akas_data.
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assert
        self.assertEqual(14, service.import_imdb_akas_data(self.spark, self.logger, self.config).count())

    def test_import_imdb_basic_data(self):
        """Test import_imdb_basic_data.
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assert
        self.assertEqual(3, service.import_imdb_basic_data(self.spark, self.logger, self.config).count())

    def test_join_imdb_basic_akas_data(self):
        """Test join_imdb_basic_akas_data.
            Using small chunks of input data and expected output data, we
            test the transformation step to make sure it's working as
            expected.
            """
        # act
        akas_df = service.import_imdb_akas_data(self.spark, self.logger, self.config)
        basic_df = service.import_imdb_basic_data(self.spark, self.logger, self.config)
        # assert
        self.assertEqual(14, service.join_imdb_basic_akas_data(akas_df, basic_df, self.logger).count())


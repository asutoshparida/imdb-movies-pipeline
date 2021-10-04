from setuptools import find_packages, setup


def readme():
    with open('README.md') as f:
        return f.read()


setup(name='imdb-movies-pipeline',
      version='0.1',
      description='imdb-movies-pipeline processor',
      long_description='',
      author='Asutosh Parida',
      author_email='ast.jva@gmail.com',
      entry_points = {
              'console_scripts': [
                  'ar = controller.AmazonReviewETL:main',
              ],
          },
      classifiers=[
        'Programming Language :: Python :: 3.7',
        'Development Status :: 4 - Beta',
        'Operating System :: POSIX :: Linux'
      ],
      packages=find_packages(),
      install_requires=[
          'importlib',
          'datetime',
          'json5',
          'logging',
          'yaml-1.3',
          'boto3',
          'pyspark==3.1.2',
          'ipython',
          'flake8',
          # 'psycopg2-binary',
          # 'apache-airflow==2.0,
          'kafka-python==2.0.2'
      ],
      dependency_links=[],
      package_data={'':['*.yaml']},
      setup_requires=['pytest-runner'],
      tests_require=['mock', 'pytest'],
      zip_safe=False)

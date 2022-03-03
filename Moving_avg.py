# -*- coding: utf-8 -*-
""" 
Created by saul.ramirez at 3/2/2022

"""
import logging
import os

import yaml
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, when
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class MovingAvg:

    _yml_config = None  # type: dict

    def __init__(self):
        logger.info("Initialize The Class Moving Avg")

    def _read_yaml_file(self) -> None:
        """Read the Yaml file configuration and set into the class variable
        _yaml_config
        Returns:
            None
        """
        logger.info('Reading config yaml file ')
        path = os.path.dirname(os.path.abspath(__file__))
        yaml_file = f"{path}/config/config.yml"
        with open(yaml_file) as f:
            data = yaml.safe_load(f)

        if data:
            self._yaml_config = data

    def _read_file(self, session, path=None):
        """"Read the csv file
        Args:
            session: spark context
            path(optional): path for the same data

        Return:
            s_df: spark data Frame with csv data loaded
        """
        logger.info("Reading csv data sample")
        if not path:
            path = self._generate_path()

        s_df = session.read.csv(path, header=self._yaml_config.get('header'))

        return s_df

    def _generate_path(self) -> str:
        """"Generate the path for the sample data in the root project directory

        Exception:
         Exception: is the file name is not defined in yaml config file.

        Return:
            path: path for the sample data as string
        """
        path = os.path.dirname(os.path.abspath(__file__))
        file = self._yaml_config.get('file_name')

        if not file:
            raise Exception("Not File define to process")

        path = f"{path}/{file}"

        return path

    def _get_windows_range(self):
        """"Generate the windows spec range

        Return:
            w_range: range
        """
        d_range = self._yaml_config.get('range_days')
        if not d_range:
            d_range = 5
        logger.info(f"Generate window range by {d_range} days ")
        w_range = Window.partitionBy('ticker').orderBy('date').rowsBetween(d_range, 0)

        return w_range

    def _filter_df(self, df):
        """"Filter the data frame byu stock values
        Args:
            df: spark data frame with prices values

        return:
            df: filtered spark data frame
        """
        logger.info("Filter data Frame by stocks")
        stocks = self._yaml_config.get('stocks')
        if not stocks:
            return df

        df_tmp = df.filter(df.ticker.isin(stocks))

        return df_tmp

    def _calculate_moving_avg(self, df):
        """"calculate the moving avg and add the field to the data frame
        Args:
            df: spark dataframe

        Returns:
            df
        """
        logger.info("Calculating Moving Avg")
        w_range = self._get_windows_range()
        df = df.select('ticker', 'close', 'date').withColumn("moving_avg", avg('close').over(w_range))
        df = df.withColumn('Row_id', F.row_number().over(Window.partitionBy("ticker").orderBy("date")))
        df = df.withColumn('moving_avg', when(df.Row_id < 7, 'N/A').otherwise(df.moving_avg)).drop('Row_id')

        return df

    @staticmethod
    def _get_spark_session(app) -> SparkSession:
        """Create the Spark context

        Args:
            app: Apps name

        Return:
            session: Spark context
        """
        session = SparkSession.builder.appName(app).getOrCreate()

        return session

    def _write_csv_data(self, df) -> None:
        """Write the data result by stock in csv format
        """
        logger.info("Writing data")
        path = os.path.dirname(os.path.abspath(__file__))
        w_path = f"{path}/result"

        df.write.option('header', self._yaml_config.get('header')).partitionBy('ticker').mode('overwrite').csv(w_path)

    def process(self) -> None:
        """Public method that have the order execution

        Return:
            None
        """
        spark = self._get_spark_session('Moving_avg')
        self._read_yaml_file()
        self._get_windows_range()
        df = self._read_file(spark)
        df_filter = self._filter_df(df)
        df_filter.count()
        df_tmp = self._calculate_moving_avg(df_filter)
        self._write_csv_data(df_tmp)

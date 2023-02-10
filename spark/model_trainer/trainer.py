import os
import logging
from pyspark.sql import SparkSession
# from pyspark.sql.functions import date_trunc, from_unixtime, col, max, min, sum, first, last, lit
from hdfs import InsecureClient
from logging.handlers import RotatingFileHandler
from file_event_consumer import FileEventConsumer

# Add your own conf dir
os.environ['HADOOP_CONF_DIR'] = os.path.abspath(os.getcwd()) + '/spark/conf'
os.environ['YARN_CONF_DIR'] = os.path.abspath(os.getcwd()) + '/spark/conf'


class ModelTrainer:
    def __init__(self) -> None:
        log_handler = RotatingFileHandler(
            f"{os.path.abspath(os.getcwd())}/spark/coin_trade/logs/analyzer.log",
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('coin_trade_analyzer')
        self.fe_consumer = FileEventConsumer()
        self.hdfs_client = InsecureClient('http://localhost:9870', user='root')
        self.spark = SparkSession.builder\
            .config("spark.app.name", "ModelTrainer")\
            .config("spark.master", "yarn")\
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0")\
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
            .config("spark.cassandra.connection.host", "172.20.0.15")\
            .config("spark.cassandra.auth.username", "cassandra")\
            .config("spark.cassandra.auth.password", "cassandra")\
            .getOrCreate()
        # Options if my computer is better...
        #  .config("spark.driver.memory", "2g")\
        #  .config("spark.executor.memory", "2g")\
        #  .config("spark.executor.instances", "2")\

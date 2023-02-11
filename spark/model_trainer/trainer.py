import os
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from train_model import LogisticRegressionPileline

from pyspark.sql.functions import col


# Add your own conf dir
os.environ['HADOOP_CONF_DIR'] = os.path.abspath(os.getcwd()) + '/spark/conf'
os.environ['YARN_CONF_DIR'] = os.path.abspath(os.getcwd()) + '/spark/conf'
EXTERNAL_JARS = 'org.apache.spark:spark-avro_2.12:3.2.2'
MODEL_SAVE_PATH = os.path.abspath(os.getcwd()) + '/spark/conf'


class ModelRunner:
    def __init__(self, logger: Logger) -> None:
        self.logger = logger
        # Need to init Spark before init training model class
        self.spark = SparkSession.builder\
            .config("spark.app.name", "ModelTrainer")\
            .config("spark.master", "yarn")\
            .config("spark.jars.packages", EXTERNAL_JARS)\
            .getOrCreate()
        self.model = LogisticRegressionPileline()
        # .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
        # .config("spark.cassandra.connection.host", "172.20.0.15")\
        # .config("spark.cassandra.auth.username", "cassandra")\
        # .config("spark.cassandra.auth.password", "cassandra")\
        # Options if my computer is better...
        #  .config("spark.driver.memory", "2g")\
        #  .config("spark.executor.memory", "2g")\
        #  .config("spark.executor.instances", "2")\

    def run_model(self, file_name: list[str]):
        try:
            df = self.spark.read.format('avro').load(file_name)
            df = df.withColumn("rating", col("rating").cast("integer"))
            train_df, test_df = df.randomSplit([0.8, 0.2], seed=100)
            self.model.train(train_df)
            self.model.predict(test_df)
        except Exception as e:
            self.logger.error(
                f'An error happened while training model: {e}')


class ModelTrainer:
    def __init__(self) -> None:
        log_handler = RotatingFileHandler(
            f"{os.path.abspath(os.getcwd())}/spark/model_trainer/logs/trainer.log",
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('model_trainer')
        self.file_event_consumer = KafkaConsumer(
            'fileEvent',
            bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
            group_id='fileEventConsummers',
            auto_offset_reset='earliest',
            enable_auto_commit=False)
        self.model_runner = ModelRunner(self.logger)

    def run(self) -> None:
        try:
            self.logger.info('Subcribe to topic fileEvent')
            while True:
                msgs_pack = self.file_event_consumer.poll(10)
                if msgs_pack is None:
                    continue

                new_files = []
                for tp, messages in msgs_pack.items():
                    for message in messages:
                        new_files.append(str(message[6], 'utf-8'))
                        break
                    break

                if new_files:
                    self.model_runner.run_model(new_files)
                    raise RuntimeError()
                    # self.file_event_consumer.commit()
        except Exception as e:
            self.logger.error(
                f'An error happened while processing messages from kafka: {e}')
        finally:
            self.file_event_consumer.close()

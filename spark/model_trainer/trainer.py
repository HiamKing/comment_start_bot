import os
import logging
import re
from logging import Logger
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession, DataFrame
from kafka import KafkaConsumer
from train_model import LogisticRegressionPileline
from pyspark.sql.functions import col
from datetime import datetime


# Add your own conf dir
os.environ['HADOOP_CONF_DIR'] = os.path.abspath(os.getcwd()) + '/spark/conf'
os.environ['YARN_CONF_DIR'] = os.path.abspath(os.getcwd()) + '/spark/conf'

EXTERNAL_JARS = 'org.apache.spark:spark-avro_2.12:3.2.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.16.0'
MODEL_SAVE_PATH = os.path.abspath(os.getcwd()) + '/spark/conf'
ES_HOST = 'elasticsearch:9200'

DATE_PREFIX = 'amazonCmtData.'
DATE_EXTENSION = '.avro'


class ModelRunner:
    def __init__(self, logger: Logger) -> None:
        self.logger = logger
        # Need to init Spark before init training model class
        self.spark = SparkSession.builder\
            .config("spark.app.name", "ModelTrainer")\
            .config("spark.master", "yarn")\
            .config("spark.jars.packages", EXTERNAL_JARS)\
            .config("es.nodes", ES_HOST)\
            .getOrCreate()
        self.model = LogisticRegressionPileline()

    def send_train_data_to_es(self, df: DataFrame, data_date: datetime) -> None:
        append_date = data_date.strftime('%m-%d-%Y')
        self.logger.info(f'Start appending data to index train_data.{append_date}/docs')
        df.write.format('es').save(f'train_data.{append_date}/docs')
        self.logger.info(f'Finish appending data to index train_data.{append_date}/docs')

    def send_predict_data_to_es(self, df: DataFrame, date_date: datetime) -> None:
        append_date = date_date.strftime('%m-%d-%Y')
        self.logger.info(f'Start appending data to index predict_data.{append_date}/docs')
        df.write.format('es').save(f'predict_data.{append_date}/docs')
        self.logger.info(f'Finish appending data to index predict_data.{append_date}/docs')

    def extract_date_from_file_name(self, file_name: str) -> datetime:
        found_datetime = re.search('[0-9]{10}', file_name).group(0)
        return datetime.fromtimestamp(int(found_datetime))

    def run_model(self, file_name: str):
        try:
            df = self.spark.read.format('avro').load(file_name)
            df = df.withColumn("rating", col("rating").cast("integer"))
            train_df, test_df = df.randomSplit([0.8, 0.2], seed=100)

            self.model.train(train_df)
            predict_df = self.model.predict(test_df)

            data_date = self.extract_date_from_file_name(file_name)
            self.send_train_data_to_es(train_df, data_date)
            self.send_predict_data_to_es(predict_df, data_date)
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
                    for file in new_files:
                        self.model_runner.run_model(file)
                        raise RuntimeError()
                    # self.file_event_consumer.commit()
        except Exception as e:
            self.logger.error(
                f'An error happened while processing messages from kafka: {e}')
        finally:
            self.file_event_consumer.close()

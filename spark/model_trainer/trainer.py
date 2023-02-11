import os
import logging
import re
import nltk
from logging import Logger
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession, DataFrame
from kafka import KafkaConsumer
from spark.model_trainer.train_model import SentimentAnalysisPileline
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import ArrayType, StringType
from datetime import datetime
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer

# Add your own conf dir
os.environ['HADOOP_CONF_DIR'] = os.path.abspath(os.getcwd()) + '/spark/conf'
os.environ['YARN_CONF_DIR'] = os.path.abspath(os.getcwd()) + '/spark/conf'


EXTERNAL_JARS = 'org.apache.spark:spark-avro_2.12:3.2.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.16.0'
MODEL_SAVE_FOLDER = os.path.abspath(os.getcwd()) + '/spark/model_trainer/trained_model'
ES_HOST = 'elasticsearch:9200'

DATE_PREFIX = 'amazonCmtData.'
DATE_EXTENSION = '.avro'

nltk.download('punkt')
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))
stemmer = SnowballStemmer("english")


def tokenize(text):
    tokens = nltk.word_tokenize(text)
    return [token.lower() for token in tokens if token.isalpha()]


# Define the UDF to stem the words
def stem_words(words):
    return [stemmer.stem(word) for word in words if word not in stop_words]


def preprocess(text):
    tokens = tokenize(text)
    return stem_words(tokens)


class ModelRunner:
    def __init__(self, logger: Logger, pretrained_file: str = '') -> None:
        self.logger = logger
        # Need to init Spark before init training model class
        self.spark = SparkSession.builder\
            .config("spark.app.name", "ModelTrainer")\
            .config("spark.master", "local[*]")\
            .config("spark.jars.packages", EXTERNAL_JARS)\
            .config("es.nodes", ES_HOST)\
            .getOrCreate()
        self.last_trained_file = pretrained_file

    def get_train_pipeline(self) -> SentimentAnalysisPileline:
        if self.last_trained_file:
            return SentimentAnalysisPileline(
                save_folder=MODEL_SAVE_FOLDER,
                load_model=True,
                save_model=True,
                pretrained_file=self.last_trained_file)
        else:
            return SentimentAnalysisPileline(
                save_folder=MODEL_SAVE_FOLDER,
                save_model=True)

    def send_train_data_to_es(self, df: DataFrame, data_date: datetime) -> None:
        append_date = data_date.strftime('%m-%d-%Y')
        if 'crawled_timestamp' not in df.columns:
            df = df.withColumn('crawled_timestamp', lit(int(datetime.now().timestamp())).cast('timestamp'))
        if 'process' in df.columns:
            df = df.drop('process')
        self.logger.info(f'Start appending data to index train_data.{append_date}/docs')
        df.write.format('es').save(f'train_data.{append_date}/docs')
        self.logger.info(f'Finish appending data to index train_data.{append_date}/docs')

    def send_predict_data_to_es(self, df: DataFrame, date_date: datetime) -> None:
        append_date = date_date.strftime('%m-%d-%Y')
        if 'crawled_timestamp' not in df.columns:
            df = df.withColumn('crawled_timestamp', lit(int(datetime.now().timestamp())).cast('timestamp'))
        self.logger.info(f'Start appending data to index predict_data.{append_date}/docs')
        df.write.format('es').save(f'predict_data.{append_date}/docs')
        self.logger.info(f'Finish appending data to index predict_data.{append_date}/docs')

    def extract_date_from_file_name(self, file_name: str) -> datetime:
        found_datetime = re.search('[0-9]{10}', file_name).group(0)
        return datetime.fromtimestamp(int(found_datetime))

    def preprocess_df(self, df: DataFrame) -> DataFrame:
        process_udf = udf(preprocess, ArrayType(StringType()))
        preprocessed_df = df.withColumn("process", process_udf(df["content"]))
        preprocessed_df = preprocessed_df.withColumn("rating", col("rating").cast("integer"))
        return preprocessed_df

    def run_model(self, file_name: str):
        df = self.spark.read.format('avro').load(file_name)
        df = self.preprocess_df(df)
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=100)

        pipeline = self.get_train_pipeline()
        self.last_trained_file = pipeline.train(train_df)
        predict_df = pipeline.predict(test_df)
        data_date = self.extract_date_from_file_name(file_name)
        self.send_train_data_to_es(train_df, data_date)
        self.send_predict_data_to_es(predict_df, data_date)


class ModelTrainer:
    def __init__(self, pretrained_file: str = '') -> None:
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
        self.model_runner = ModelRunner(self.logger, pretrained_file)

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

                if new_files:
                    for file in new_files:
                        self.model_runner.run_model(file)
                    self.file_event_consumer.commit()
        except Exception as e:
            self.logger.error(
                f'An error happened while processing messages from kafka: {e}')
        finally:
            self.file_event_consumer.close()

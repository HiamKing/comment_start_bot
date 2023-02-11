import logging
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import os


class AmazonProducer:
    def __init__(self) -> None:
        log_handler = RotatingFileHandler(
            f'{os.path.abspath(os.getcwd())}/kafka/amz_producer/logs/producer.log',
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('amazon_producer')

        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
            client_id='amazon_producer')

    def message_handler(self, message: dict) -> None:
        #  Message from amazon comments crawler
        try:
            crawled_timestamp = int(datetime.now().timestamp())
            amazon_cmt_data = f"{message['title']}[this_is_sep]{message['rating']}[this_is_sep]{message['content']}[this_is_sep]{message['category']}[this_is_sep]{message['asin']}[this_is_sep]{message['price']}[this_is_sep]{crawled_timestamp}"
            self.producer.send('amazonCmtData', bytes(amazon_cmt_data, encoding='utf-8'))
            self.producer.flush()
        except KafkaError as e:
            self.logger.error(f'An Kafka error happened: {e}')
        except Exception as e:
            self.logger.error(f'An error happened while pushing message to Kafka: {e}')

import logging
import os
import tempfile
import json
from logging.handlers import RotatingFileHandler
from kafka import KafkaConsumer
from hdfs import InsecureClient
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from file_event_producer import FileEventProducer
from datetime import datetime


AMAZON_MESSAGE_SCHEMA = avro.schema.parse(json.dumps({
    'type': 'record',
    'name': 'AmazonComment',
    'fields': [
        {'name': 'title', 'type': 'string'},
        {'name': 'rating',  'type': 'float'},
        {'name': 'content', 'type': 'string'},
        {'name': 'category', 'type': 'string'},
        {'name': 'asin', 'type': 'string'},
        {'name': 'price', 'type': ['float', 'null']},
        {'name': 'crawled_timestamp', 'type': 'long'}
    ]
}))

file_event_producer = FileEventProducer()


class AmazonConsumer:
    def __init__(self) -> None:
        log_handler = RotatingFileHandler(
            f'{os.path.abspath(os.getcwd())}/kafka/amz_consumer/logs/consumer.log',
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('amazon_consumer')
        self.consumer = KafkaConsumer(
            'amazonCmtData',
            bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
            group_id='amazonCmtDataConsummers',
            auto_offset_reset='earliest',
            enable_auto_commit=False)
        self.hdfs_client = InsecureClient('http://localhost:9870', user='root')

    def flush_to_hdfs(self, tmp_file_name: str) -> None:
        current_time = datetime.now()
        hdfs_filename = '/amazonCmtData/' +\
            str(current_time.year) + '/' +\
            str(current_time.month) + '/' +\
            str(current_time.day) + '/'\
            f'amazonCmtData.{int(round(current_time.timestamp()))}.avro'

        self.logger.info(
            f'Starting flush file {tmp_file_name} to hdfs')
        flush_status = self.hdfs_client.upload(hdfs_filename, tmp_file_name)
        if flush_status:
            self.logger.info(f'Flush file {tmp_file_name} to hdfs as {hdfs_filename} successfully')
        else:
            raise RuntimeError(f'Failed to flush file {tmp_file_name} to hdfs')
        file_event_producer.message_handler(hdfs_filename)
        self.consumer.commit()

    def recreate_tmpfile(self) -> tuple[tempfile._TemporaryFileWrapper, DataFileWriter]:
        tmp_file = tempfile.NamedTemporaryFile()
        writer = DataFileWriter(open(tmp_file.name, 'wb'), DatumWriter(), AMAZON_MESSAGE_SCHEMA)
        return tmp_file, writer

    def close_tmpfile(self, tmp_file: tempfile._TemporaryFileWrapper, writer: DataFileWriter) -> None:
        tmp_file.close()
        writer.close()

    def run(self) -> None:
        try:
            tmp_file, writer = self.recreate_tmpfile()
            self.logger.info('Subcribe to topic amazonCmtData')
            while True:
                msgs_pack = self.consumer.poll(10)
                if msgs_pack is None:
                    continue

                for tp, messages in msgs_pack.items():
                    for message in messages:
                        decode_msg = str(message[6], 'utf-8').split('[this_is_sep]')
                        amz_cmt_msg = {
                            'title': decode_msg[0],
                            'rating': float(decode_msg[1]),
                            'content': decode_msg[2],
                            'category': decode_msg[3],
                            'asin': decode_msg[4],
                        }
                        if decode_msg[5] != 'None':
                            amz_cmt_msg['price'] = float(decode_msg[5].replace(',', ''))
                        if len(decode_msg) < 7:
                            amz_cmt_msg['crawled_timestamp'] = int(datetime.now().timestamp())
                        else:
                            amz_cmt_msg['crawled_timestamp'] = int(float(decode_msg[6]))
                        writer.append(amz_cmt_msg)
                        writer.flush()

                # File size > 10mb flush to hdfs
                if writer.sync() > 10485760:
                    self.flush_to_hdfs(tmp_file.name)
                    self.close_tmpfile(tmp_file, writer)
                    tmp_file, writer = self.recreate_tmpfile()
        except Exception as e:
            self.logger.error(
                f'An error happened while processing messages from kafka: {e}')
        finally:
            tmp_file.close()
            self.consumer.close()

# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from pykafka import KafkaClient
from scrapy import log
from scrapy.exceptions import DropItem
from scrapy.utils.serialize import ScrapyJSONEncoder

from scrapy_learning import settings


class ScrapyLearningPipeline(object):
    def __init__(self):
        kafka_ip_port = settings.KAFKA_IP_PORT
        kafka_topic = settings.KAFKA_TOPIC
        if len(kafka_ip_port) == 1:
            kafka_ip_port = kafka_ip_port[0]
        else:
            if isinstance(kafka_ip_port, list):
                kafka_ip_port = ",".join(kafka_ip_port)
            else:
                kafka_ip_port = kafka_ip_port
        self._client = KafkaClient(hosts=kafka_ip_port)
        self._producer = self._client.topics[kafka_topic.encode(encoding="UTF-8")].get_producer()
        self._encoder = ScrapyJSONEncoder()

    def process_item(self, item, spider):
        item = dict(item)
        item['spider'] = spider.name
        msg = self._encoder.encode(item)
        print(msg)
        self._producer.produce(msg.encode(encoding="UTF-8"))
        # self._producer.produce(item['url'].encode(encoding="UTF-8"))
        return item

    def close_spider(self,spider):
        self._producer.stop()

class MongoDBPipeline(object):
    def __init__(self):
        connection = pymongo.MongoClient(settings.MONGODB_SERVER, settings.MONGODB_PORT)
        db = connection[settings.MONGODB_DB]
        db.authenticate(settings.MONGODB_USER, settings.MONGODB_PWD)
        self.collection = db[settings.MONGODB_COLLECTION]

    def process_item(self,item,spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing{0}".format(data))
        if valid:
            self.collection.insert(dict(item))
            log.msg('question added to mongodb database!', level=log.DEBUG, spider=spider)
        return item

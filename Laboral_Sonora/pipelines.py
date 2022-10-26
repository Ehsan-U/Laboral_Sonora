# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient
from scrapy.exceptions import DropItem


class MongoStore(object):
    collection = 'test'  # in non-relational-db we called table as collection.

    def __init__(self):
        # define url where mongo listening.
        # self.mongo_uri = 'mongodb://localhost:27017'
        self.mongo_uri = 'mongodb+srv://root:Sb3nmKs5rMdIhXya@cluster1.409m5rv.mongodb.net/?retryWrites=true&w=majority'
        # name of the database
        self.mongo_db = 'Sonora'

    # when scrapy spider will launch (opened), connection with database will be established.
    def open_spider(self,spider):
        # connecting to mongo server
        self.client = MongoClient(self.mongo_uri)
        # creating database there (first run) and then connect to it.
        self.db = self.client[self.mongo_db]

    # when scrapy spider will closed , connection with database will also closed.
    def close_spider(self,spider):
        self.client.close()

    # during the spider open and close interval, we will process the data that returned by spider.
    def process_item(self, item, spider):
        # check whether that item already exist in DB
        if self.db[self.collection].count_documents({'actor':item.get("actor"),'fecha':item.get("fecha"),'expediente':item.get("expediente"),'entidad':item.get('entidad'), 'juzgado':item.get('juzgado')})==1:
            raise DropItem('Item already exist so drop it')
        else:	
            # it takes dict
            print(f"\r [+] {item['fecha']}:{item['entidad']}", end='')
            self.db[self.collection].insert_one(dict(item))
            
        # so if i comment below return then None will be returned
        return item
# This Python file uses the following encoding: utf-8
import sys
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from pprint import pprint

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

docs = []
cur = db.image.find({})
for doc in cur:
    url = doc.get('210x210', {}).get('url')
    key = {
        'logo': doc.get('logo', ''),
        'src': doc.get('src', '')
    }
    data = {
        '$set': {
            'url': url,
            'dt': doc.get('210x210', {}).get('dt')
        }
    }
    if url:
        docs.append(UpdateOne(key, data, upsert=True))

    if len(docs) > 10000:
        try:
            result = db.images.bulk_write(docs)
            pprint(result.bulk_api_result)
        except BulkWriteError as bwe:
            pprint(bwe.details)
        docs = []

try:
    result = db.images.bulk_write(docs)
    pprint(result.bulk_api_result)
except BulkWriteError as bwe:
    pprint(bwe.details)

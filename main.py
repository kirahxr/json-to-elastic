import json, random, string, threading
from elasticsearch import Elasticsearch
from datetime import datetime

with open('data/data.json') as f:
    data_bank = json.load(f)

es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])


def insert_elastic():
    for length in range(len(data_bank)):
        result = es.index(index='activate_order_deliveries_tmp', ignore=400,
                          doc_type='_doc', id=random_string(), body=data_bank[length])

        print(
            "[+] Status Insert =>", result['result'],
            "|| id:", result['_id'],
            "|| Success:", result['_shards']['successful'],
            "|| Failed:", result['_shards']['failed'],
            "|| Date:", datetime.now()
        )


def random_string():
    x = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    return x


def split_processing(num):
    threads = []
    for i in range(len(num)):
        threads.append(threading.Thread(target=insert_elastic))
        threads[-1].start()

    for t in threads:
        t.join()


if __name__ == "__main__":
    num = input("How many threads? ")
    split_processing(num)

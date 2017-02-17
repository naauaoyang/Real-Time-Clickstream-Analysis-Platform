from kafka import KafkaProducer
import sys
import time
import csv
import json
from datetime import datetime
import pandas as pd
import numpy as np


class Producer(object):

    def run(self, data_path, bootstrap_servers):
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        df = pd.read_csv(data_path)
        for msg_cnt in range(3000000):
            i = np.random.randint(1000)
            data = df.iloc[i:i+1]
            message_info = {'prev_title': df.get_value(i, 'prev_title'),
                            'curr_title': df.get_value(i, 'curr_title'),
                            'type': df.get_value(i, 'type'),
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
            message_info = json.dumps(message_info, encoding='utf-8') 
            print message_info
            producer.send('clickstream', message_info)
            time.sleep(0.01)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: producer <data_path> <bootstrap_servers>")
        exit(-1)
    prod = Producer()
    prod.run(sys.argv[1], sys.argv[2])


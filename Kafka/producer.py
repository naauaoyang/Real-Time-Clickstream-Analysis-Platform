from kafka import KafkaProducer
import sys
import time
import csv
import json
from datetime import datetime
import pandas as pd
import numpy as np


class Producer(object):

    def run(self, data_path):
        producer = KafkaProducer(bootstrap_servers='ec2-34-192-175-58.compute-1.amazonaws.com:9092')
        df = pd.read_csv(data_path)
        for msg_cnt in range(100):
            i = np.random.randint(1000)
            data = df.iloc[i:i+1]
            message_info = {'prev_title': df.get_value(i, 'prev_title'),
                            'curr_title': df.get_value(i, 'curr_title'),
                            'type': df.get_value(i, 'type'),
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
            #message_info = json.dumps(message_info, encoding='utf-8', ensure_ascii=False) 
            message_info = json.dumps(message_info, encoding='utf-8') 
            print message_info
            producer.send('clickstream', message_info)
            time.sleep(0.1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: producer <data_path>")
        exit(-1)
    prod = Producer()
    prod.run(sys.argv[1])


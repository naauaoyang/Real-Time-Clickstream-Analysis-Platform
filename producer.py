from kafka import KafkaProducer
import time
import csv
import json
from datetime import datetime


class Producer(object):

    def run(self):
        producer = KafkaProducer(bootstrap_servers='ec2-34-192-175-58.compute-1.amazonaws.com:9092')
        with open("2015_02_clickstream.tsv", "rb") as f:
            reader = csv.reader(f, delimiter="\t")
            msg_cnt = 0
            reader.next()
            for line in reader:
                prev_id = line[0]
                curr_id = line[1]
                n = line[2]
                prev_title = line[3]
                curr_title = line[4]
                type_ = line[5]
                
                #str_fmt = "{} {} {} {} {} {}"
                #message_info = str_fmt.format(prev_id, curr_id, n, prev_title, curr_title, type_)
                
                #'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                message_info = {'prev_id': line[0], 
                                'curr_id': line[1], 
                                'n': line[2], 
                                'prev_title': line[3], 
                                'curr_title': line[4], 
                                'type': line[5],
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                #message_info = json.dumps(message_info, encoding='utf-8', ensure_ascii=False) 
                message_info = json.dumps(message_info, encoding='utf-8') 
                print message_info
                producer.send('wiki', message_info)
                time.sleep(0.1)
 
                msg_cnt += 1
                if msg_cnt > 1000:
                    break

def main():
    prod = Producer()
    prod.run()

if __name__ == "__main__":
    main()

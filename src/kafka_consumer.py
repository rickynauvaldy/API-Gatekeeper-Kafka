from kafka import KafkaConsumer
from json import loads

def MyKafkaConsumer():
    consumer = KafkaConsumer(
    'data_test',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        print(message.value)

if __name__ == '__main__':
    MyKafkaConsumer()
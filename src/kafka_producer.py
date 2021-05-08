# Don't forget to set environment first
# in windows:
# set FLASK_APP=main.py
# set FLASK_ENV=debug
# flask run

# import libs
from flask import Flask, request, abort
import json

## This lib will be used later as validators
## from flask_inputs.validators import JsonSchema

from pykafka import KafkaClient
import time

# initiate Flask instance
app = Flask(__name__)

# POST route
@app.route('/', methods=['POST'])
def create_task():
    # Validate if request is JSON
    if not request.json:
        # return response 400 and abort
        abort(400)
    
    client = KafkaClient(hosts="localhost:9092")
    topic = client.topics['data_test']
    producer = topic.get_sync_producer()

    producer.produce(bytes(json.dumps(request.json), encoding='utf-8'))
    time.sleep(1)

    # return request.json with 201 response code
    return "OK", 201

if __name__ == "__main__":
    app.run(debug=True)
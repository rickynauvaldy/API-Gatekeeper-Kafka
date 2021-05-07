# API-Gatekeeper-Kafka
 Create API to validate JSON input, send to Kafka when valid, and write it into database

# Description
As a data engineer, collaboration with back end team is inevitable. In this case, the back end team built a system to capture user activity data, while <b>the data engineer</b> will transport the data in real time using <a href="https://kafka.apache.org/">Apache Kafka</a> and put it in a database (in this case, <a href="https://www.postgresql.org/">PostgreSQL</a> / <a href="https://cloud.google.com/bigquery">Google BigQuery</a> [work in progress]). As accessing data directly to the database might harm the database performance, we will build an API to accept the back end team user activity in JSON format, while it's validated first as there are possibilty that the payload is non-standard, acting as a <b>Gatekeeper</b>.

# Prerequisites
- List of requirements are available in the `requirements.txt` and can be installed by running `pip install -r requirements.txt`

# Data
The example of user activity data in JSON format is available in the `resources` folder with the name of `example.json`.

The data looks like the following:

```
{
  "change": [
    {
      "kind": "insert",
      "table": "mytable",
      "columnnames": [
        "a",
        "b",
        "c"
      ],
      "columntypes": [
        "INTEGER",
        "TEXT",
        "TEXT"
      ],
      "columnvalues": [
        1,
        "Backup and Restore",
        "2018-03-27 11:58:28.988414"
      ]
    },
    {
      "kind": "insert",
      "table": "mytable",
      "columnnames": [
        "a",
        "b",
        "c"
      ],
      "columntypes": [
        "INTEGER",
        "TEXT",
        "TEXT"
      ],
      "columnvalues": [
        2,
        "Test 2",
        "2019-03-27 10:13:13.948514"
      ]
    },
    {
      "kind": "insert",
      "table": "mytable",
      "columnnames": [
        "a",
        "b",
        "c"
      ],
      "columntypes": [
        "INTEGER",
        "TEXT",
        "TEXT"
      ],
      "columnvalues": [
        3,
        "Test 3",
        "2019-04-28 10:24:30.183414"
      ]
    },
    {
      "kind": "delete",
      "table": "mytable",
      "oldkeys": {
        "keynames": [
          "a",
          "c"
        ],
        "keytypes": [
          "INTEGER",
          "TEXT"
        ],
        "keyvalues": [
          1,
          "2018-03-27 11:58:28.988414"
        ]
      }
    },
    {
      "kind": "delete",
      "table": "mytable",
      "oldkeys": {
        "keynames": [
          "a",
          "c"
        ],
        "keytypes": [
          "INTEGER",
          "TEXT"
        ],
        "keyvalues": [
          3,
          "2019-04-28 10:24:30.183414"
        ]
      }
    }
  ]
}
```

# Flow
![Alt text](img/flow.jpg?raw=true "Postman")
- JSON formatted input will be provided to API
- API will validate the input
- Validated message will be produce as a Kafka message
- Consumer will consume Kafka message
- Consumed message will be written to database (<i>work in progress</i>)

# Running the Program
- Make sure that all the prerequisites are satisfied
- Start a <a href="https://zookeeper.apache.org/">Zookeeper</a> server and a Kafka server. For running in Windows, you might want to refer to <a href="https://dzone.com/articles/running-apache-kafka-on-windows-os">this page</a>.

## Running `kafka_producer.py`
- As the code configuration is still hard-coded, things that you might want to change are:
  1. Hosts in the `client = KafkaClient(hosts="localhost:9092")` line
  2. Name of the topic in the `topic = client.topics['data_test']` line
- Ensure that the environment variables of `FLASK_APP` and `FLASK_ENV` are set. On Windows, you can do the following in the Command Prompt (CMD):
```
set FLASK_APP=kafka_producer.py
set FLASK_ENV=debug
```
- Run the program by executing this command:
```
flask run
```

## Running `kafka_consumer.py`
- As the code configuration is still hard-coded, things that you might want to change are:
  1. topic_name (e.g. `data_test`)
  2. bootstrap_servers (i.e. `bootstrap_servers=['localhost:9092']`)
  ```
  consumer = KafkaConsumer(
    'data_test',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))
  ```
- Run the program by executing this command:
```
python kafka_consumer.py
```

## Sending JSON request to the API
- This API will accept `POST` request with JSON body
- Make sure that the address used is the same as the API address (e.g. `localhost:5000` as the default Flask address)
- One of the tools that could be used for creating a JSON POST request is <a href="https://www.postman.com/">Postman</a> (Please also set the Headers with `Content-Type` key and `application/json` value).![Alt text](img/postman.jpg?raw=true "Postman")

# Output
- If the Flask application of `kafka_producer.py` runs well, it will gives an output as follows:
![Alt text](img/kafka_producer.jpg?raw=true "Kafka Producer")
- If the `kafka_consumer.py` runs well, it will gives an output as follows (empty when request is not sent already):
![Alt text](img/kafka_consumer.jpg?raw=true "Kafka Consumer")
# Notes
- The configuration is still hard-coded in the script. It will be better if it's stored in a configuration file.
- The development of the program is not yet finished as it's not added with the JSON content validator in the `kafka_producer.py`, and it doesn't process the JSON message yet to be stored in the database in the `kafka_consumer.py`. It will be updated soon.
- This code is made as a Week 4 Task in the Academi by blank-space Data Engineering Track Program (https://www.blank-space.io/program)

# Contact
For further information, you might want to reach me to ricky.nauvaldy@gmail.com
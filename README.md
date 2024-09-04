run kafka server - sudo systemctl status kafka
start -python3 producer.python3
start -python3 consumer.py


---------
gsutil notification create -f json -t gs-to-bq-topic -e FINALIZE gs://covidstreamming

The above command make notify to topic that new file came[watch dog]

----------------project---------------

python kafka produce to produce streaming data from csv
consumer will make it valid and write to bucket

when new file uploaded to file then pub suub will trigger my cloud function

cloud function will read data and validated it by find duplicate and upload data to bigquery



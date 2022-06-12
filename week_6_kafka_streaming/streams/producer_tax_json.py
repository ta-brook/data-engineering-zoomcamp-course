import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open('../avro_example/data/rides.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"vendorId": int(row[0])}
    value = {"vendorId": int(float(row[0])), "passenger_count": int(float(row[7])), "trip_distance": float(row[8]), "payment_type": int(float(row[17])), "total_amount": float(row[16])}
    producer.send('datatalkclub.yellow_taxi_ride.json', value=value, key=key)
    print("producing")
    sleep(1)
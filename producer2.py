from kafka import KafkaProducer
from time import sleep
import csv, json

SERVER = ['localhost:9092']
FILENAME = '/home/magariou/Desktop/P_Synthese/New_Datas/log2_non_t.csv'
TOPIC = 'second_log_server'

SERIALIZER = lambda x: json.dumps(x).encode("UTF-8")

# Connexion à Kafka
producer = KafkaProducer(bootstrap_servers = SERVER, value_serializer=SERIALIZER)

with open(FILENAME, newline='') as csvFile:
    datas = csv.DictReader(csvFile)
    for row in datas:
        producer.send(TOPIC, value=row, key=str(row['response']).encode())
        producer.flush()

        print("********Transmission des messages en cours***************")
        sleep(1)
    
    print("************TRANSMISSION DES MESSAGES TERMINEE*************")
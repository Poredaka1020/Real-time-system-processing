from kafka import KafkaConsumer
import json
from time import sleep


SERVER = ['localhost:9092']
TOPIC1 = 'first_log_server'

DESERIALIZER = lambda x: json.loads(x)

# On se connecte à kafka 
consumer = KafkaConsumer(TOPIC1, bootstrap_servers = SERVER, value_deserializer = DESERIALIZER,  auto_offset_reset='earliest', group_id="log_analysis1")


# Lire le fichier et voir ce qui a été écrit
# for message in consumer:
#     print(message.value)



# Création d'un producteur pour écrire dans la sortie du consumer dans le topic agrégé
from kafka import KafkaProducer

Topic_Agg = 'aggregating_log_servers'

SERIALIZER = lambda x: json.dumps(x).encode("UTF-8")

# Connexion au producteur kafka
producer = KafkaProducer(bootstrap_servers = SERVER)
for row in consumer:
    producer.send(Topic_Agg, value=json.dumps(row.value).encode("UTF-8"))
    producer.flush()

    print("********Réception***************")
    sleep(1)

print("**********Tous les messages ont été lus*******************")
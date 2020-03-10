from kafka import KafkaProducer
import json

kafka_prod = KafkaProducer(bootstrap_servers='172.31.0.3:9092')
cities = list()

with open('../city_population.json', 'r') as f:
    cities = json.load(f)

for city in cities:
    kafka_prod.send("city_population", json.dumps(city).encode('utf-8'))
    #print (str(city["City"]) + " in the position " + str(cities.index(city)) + " has been sent to Kafka")
    #time.sleep(0.01)

print("All messages has been sent to Kafka")

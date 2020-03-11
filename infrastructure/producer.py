from kafka import KafkaProducer
import json
import docker

#Resolving zookeeper IP address
client = docker.DockerClient()
kafka_container = client.containers.get("kafka")
kafka_ip = kafka_container.attrs['NetworkSettings']['Networks']['spark_streaming_internal']['IPAddress']
kafka_port = kafka_container.attrs['NetworkSettings']['Ports']['9092/tcp'][0]['HostPort']

kafka_prod = KafkaProducer(bootstrap_servers=kafka_ip + ':' + kafka_port)
cities = list()

with open('../city_population.json', 'r') as f:
    cities = json.load(f)

for city in cities:
    kafka_prod.send("city_population", json.dumps(city).encode('utf-8'))

print("All messages has been sent to Kafka")

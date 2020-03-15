import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import docker

#Resolving zookeeper IP address
client = docker.DockerClient()
zk_container = client.containers.get("zookeeper")
zk_ip = zk_container.attrs['NetworkSettings']['Networks']['spark_streaming_internal']['IPAddress']
zk_port = zk_container.attrs['NetworkSettings']['Ports']['2181/tcp'][0]['HostPort']

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
sc = SparkContext("local[10]", appName="PythonSparkStreamingKafka")
#sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 10)
kafkaStream = KafkaUtils.createStream(ssc, zk_ip + ":" + zk_port, "cities-consumer-group", {'city_population': 1})
cities_dstream = kafkaStream.map(lambda v: json.loads(v[1]))


def create_tuple(rdd):
    if rdd["City"] is not None and rdd["Year"] is not None:
        return (rdd["City"], rdd["Year"])


def simplify(rdd):
    if rdd["City"] is not None and rdd["Country or Area"] is not None and rdd["Value"] is not None and rdd["Year"] is not None:
        return {"City": rdd["City"],
                "Country": rdd["Country or Area"],
                "Value": rdd["Value"],
                "Year": rdd["Year"]}


#South American countries
sa_countries = ["Argentina", "Brazil", "Bolivia (Plurinational State of)", "Colombia", "Chile", "Ecuador", "Paraguay",
                "Peru", "Venezuela (Bolivarian Republic of)", "Uruguay"]

#South American Cities
cities_raw = cities_dstream\
    .filter(lambda rdd: rdd["Country or Area"] in sa_countries)\
    .filter(lambda rdd: rdd["City type"] == "City proper")\
    .transform(lambda rdd: rdd.sortBy(lambda city: city["City"]))\
    .map(lambda rdd: simplify(rdd))
cities_raw.pprint()

#Latest census year by city
census_year = cities_raw\
    .map(lambda rdd: (rdd["City"], int(rdd["Year"])))\
    .reduceByKey(max)
census_year.pprint()

#Total number of cities
total_cities = census_year\
    .count()\
    .pprint()

#Number of cities by country
cities_by_country = cities_raw\
    .map(lambda rdd: (rdd["Country"], rdd["City"]))\
    .transform(lambda rdd: rdd.distinct())\
    .map(lambda rdd: rdd[0])\
    .countByValue()\
    .transform(lambda rdd: rdd.sortBy(lambda city: -city[1]))\
    .pprint()

#Cities sortedby population
most_populated_cities = cities_raw\
    .map(lambda rdd: (rdd["City"], float(rdd["Value"])))\
    .reduceByKey(max)\
    .pprint()

#Most populated city by country


#Total popilation by Country


ssc.start()
ssc.awaitTermination()
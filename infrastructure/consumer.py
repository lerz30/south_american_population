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


def extract_values(city):
    values = []
    if city["City"] is not None:
        values.append(city["City"])
    if city["Year"] is not None:
        values.append(city["Year"])
    return values


def create_filters(rdd):
    values = rdd.map(lambda city: extract_values(city)).collect()
    aux = {}
    for i in values:
        if len(aux) == 0:
            aux[i[0]] = i[1]
        else:
            if i[0] in aux:
                if i[1] > aux[i[0]]:
                    aux[i[0]] = i[1]
            else:
                aux[i[0]] = i[1]
    return aux


def process(rdd):
    filter = sc.broadcast(create_filters(rdd))
    print(filter.value.items())
    rdd = rdd.filter(lambda city: city["City"] in filter.value.items())
    return rdd


def add_field(city):
    if city["City"] is not None and city["Year"] is not None:
        city["Filter"] = str(city["City"]) + ", " + str(city['Year'])
    return city


#South American countries
sa_countries = ["Argentina", "Brazil", "Bolivia (Plurinational State of)", "Colombia", "Chile", "Ecuador", "Paraguay",
                "Peru", "Venezuela (Bolivarian Republic of)", "Uruguay"]

#South American Cities
sa_cities_raw = cities_dstream\
    .filter(lambda rdd: rdd["Country or Area"] in sa_countries)\
    .filter(lambda rdd: rdd["City type"] == "City proper")\
    .transform(lambda rdd: rdd.sortBy(lambda city: city["City"]))
sa_cities_raw.pprint()

#DStream with cities + new field
sa_cities_wfilter = sa_cities_raw\
    .map(lambda city: (add_field(city)))
sa_cities_wfilter.pprint()

#Total South American cities
sa_cities_count = sa_cities_raw.count()
sa_cities_count.pprint()

#Number of cities by country
sa_cities_country_count = sa_cities_raw\
    .map(lambda rdd: rdd["Country or Area"])\
    .countByValue()\
    .transform(lambda rdd: rdd.sortBy(lambda city: -city[1]))
sa_cities_country_count.pprint()

#Cities that are repeated over 5 times
sa_cities_country_count = sa_cities_raw\
    .map(lambda rdd: rdd["City"])\
    .countByValue()\
    .filter(lambda rdd: int(rdd[1] > 5))\
    .transform(lambda rdd: rdd.sortBy(lambda city: -city[1]))
sa_cities_country_count.pprint()

aux = sa_cities_raw\
    .transform(lambda rdd: rdd.sortBy(lambda city: city["City"]))\
    .transform(lambda rdd: process(rdd))
aux.pprint()


ssc.start()
ssc.awaitTermination()
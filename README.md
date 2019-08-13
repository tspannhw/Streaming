[![Quality gate](https://sonarcloud.io/api/project_badges/quality_gate?project=zBrainiac_Streaming)](https://sonarcloud.io/dashboard?id=zBrainiac_Streaming)
# Streaming

## Concept:  



### Use cases:  
#### Merge two data steams - trx with the latest fx rate:  

![join streams based on "fx"](https://github.com/zBrainiac/Streaming/blob/master/Images/FlumeJoinStreams.png?raw=true "Title")


Merged result:
```
{
   "5130-2220-4900-6727":{
      "cc_type":"Visa",
      "shop_id":4,
      "fx":"EUR",
      "amount_orig":86.82,
      "fx_account":"CHF",
      "cc_id":"5130-2220-4900-6727",
      "shop_name":"Ums Eck",
      "timestamp":1565604610745
   },
   "EUR":{
      "fx":"EUR",
      "fx_rate":0.91,
      "timestamp":1565604610729
   }
}
```

#### Aggregation on continuous data stream:  


## Test setup:
All test can run on a localhost

### Requirements:  
- local installation of the latest Apache Kafka (e.g. on infra/kafka_2.12-2.3.0)
- up-to-date IDE such as Intellij IDEA

### Test Environment:  
```
cd /Users/xxx/infra/kafka_2.12-2.3.0  
bin/zookeeper-server-start.sh config/zookeeper.properties  
bin/kafka-server-start.sh config/server.properties  


./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic trx &&  
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic fx &&  
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092 &&  
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fx
``` 

### Test Data Generator:
The project provides two test-data generators:  
- KafkaJsonProducer_trx - generating fake Credit Card Transaction  
```
{"timestamp":1565604610745,"shop_id":4,"shop_name":"Ums Eck","cc_type":"Visa","cc_id":"cc_id":"5130-2220-4900-6727","amount_orig":86.82,"fx":"EUR","fx_account":"CHF"}
```  
- KafkaJsonProducer_fx - generating fake Foreign Exchange Rates for some currencies  
```  
{"timestamp":1565604610729,"fx":"EUR","fx_rate":0.91}
```



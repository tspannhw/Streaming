[![Quality gate](https://sonarcloud.io/api/project_badges/quality_gate?project=zBrainiac_Streaming)](https://sonarcloud.io/dashboard?id=zBrainiac_Streaming) & 
[![Build Status](https://travis-ci.org/zBrainiac/Streaming.svg?branch=master)](https://travis-ci.org/zBrainiac/Streaming)
# Streaming

## Concept:  
Presentation of streaming applications based on credit card transactions and FX rate stream

![overview](https://github.com/zBrainiac/Streaming/blob/master/Images/Overview.png?raw=true "Title")
  
### Use cases:  
#### Use case 1 - "count"
Start small - counting transactions per shop (group by) 

![overview](https://github.com/zBrainiac/Streaming/blob/master/Images/uc1.png?raw=true "Title")  

logical dataflow:  
![logical dataflow](https://github.com/zBrainiac/Streaming/blob/master/Images/uc1_dataflow.png?raw=true "Title")  
 
class: KafkaCount_trx_per_shop  
```
DataStream <Tuple2<String, Integer>> aggStream = trxStream
    .flatMap(new trxJSONDeserializer())
    // group by shop_name and sum their occurrences
    .keyBy(0)  // shop_name
    .sum(1);
```
JSON input stream:
```
{"timestamp":1566829043004,"cc_id":"5123-5985-1943-6358","cc_type":"Maestro","shop_id":3,"shop_name":"SihlCity","fx":"USD","fx_account":"CHF","amount_orig":40.0}
```

JSON output stream:
```
{"SihlCity":83}
```
  
#### Use case 2 - "sum"

![overview](https://github.com/zBrainiac/Streaming/blob/master/Images/uc2.png?raw=true "Title")  

logical dataflow:  
![logical dataflow](https://github.com/zBrainiac/Streaming/blob/master/Images/uc2_dataflow.png?raw=true "Title")

class: KafkaSum_ccid_trx_fx  

```
DataStream <Tuple2<String, Integer>> aggStream = trxStream
    .flatMap(new trxJSONDeserializer())
    // group by "cc_id" AND "fx" and sum their occurrences
    .keyBy(0, 1)  // shop_name
    .sum(4);
```
JSON input stream:
```
{"timestamp":1566829043004,"cc_id":"5123-5985-1943-6358","cc_type":"Maestro","shop_id":3,"shop_name":"SihlCity","fx":"USD","fx_account":"CHF","amount_orig":40.0}
```

JSON output stream:
```
{"Maestro":"EUR":"USD":101.81}
```
  
#### Use case 3 - "merge two streams"
Merge two data steams - trx with the latest fx rate:  

![overview](https://github.com/zBrainiac/Streaming/blob/master/Images/uc3.png?raw=true "Title")  

logical dataflow:  
![logical dataflow](https://github.com/zBrainiac/Streaming/blob/master/Images/uc3_dataflow.png?raw=true "Title")

JSON input stream:
```
{"timestamp":1566829043004,"cc_id":"5123-5985-1943-6358","cc_type":"Maestro","shop_id":3,"shop_name":"SihlCity","fx":"USD","fx_account":"CHF","amount_orig":40.0}
{"timestamp":1566829830600,"fx":"USD","fx_target":"CHF","fx_rate":1.03}
```


Merged result:
```
{
   "fx":{
      "fx_target":"CHF",
      "fx":"USD",
      "fx_rate":1.03,
      "timestamp":1566829830600
   },
   "trx":{
      "cc_type":"SihlCity",
      "shop_id":2,
      "fx":"USD",
      "amount_orig":40.00,
      "fx_account":"CHF",
      "cc_id":"5123-5985-1943-6358",
      "shop_name":"SihlCity",
      "timestamp":1566829043004
   }
}
```
  
#### Use case 4 - FX Risk Calculation on historical data

![overview](https://github.com/zBrainiac/Streaming/blob/master/Images/uc4.png?raw=true "Title")

 
 
 
 #### Use case 5 - "check on duplicated cc trx within in a window"  
 build fingerprint of the "cc transaction" stream, keep fingerprint in a window for {30 sec}.  
 filter out if the fingerprint is unique within the window - if the fingerprint occurs several times send alarm event  
 
 ![overview](https://github.com/zBrainiac/Streaming/blob/master/Images/uc5.png?raw=true "Title")  
 
 logical dataflow:  
 ![logical dataflow](https://github.com/zBrainiac/Streaming/blob/master/Images/uc5_dataflow.png?raw=true "Title")
 
 JSON input stream:
 ```
 {"timestamp":1566829043004,"cc_id":"5123-5985-1943-6358","cc_type":"Maestro","shop_id":3,"shop_name":"SihlCity","fx":"USD","fx_account":"CHF","amount_orig":40.0}
 ```
 
 Alarm in case of a duplicated cc trx  
  ```
 {"5125-6782-8503-3405"_"CHF"_"CHF"_54.8:2}
 ```
 
 
 
## Test setup:
All test can run on a localhost

### Requirements:  
- local installation of the latest Apache Kafka (e.g. on infra/kafka_2.12-2.3.0)
- up-to-date IDE such as Intellij IDEA

### Test Environment:  
```
cd infra/kafka_2.12-2.3.0  
bin/zookeeper-server-start.sh config/zookeeper.properties  
bin/kafka-server-start.sh config/server.properties  


./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic trx &&  
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic fx &&  
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092 &&  
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trx
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fx
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1
``` 

### Test Data Generator:
The project provides two test-data generators:  
- KafkaJsonProducer_trx - generating fake Credit Card Transaction and publishing the JSON string on Kafka / topic = trx 
- run with: 
$ java -classpath streaming-1.0-SNAPSHOT-jar-with-dependencies.jar producer.KafkaJsonProducer_trx  
$ java -classpath streaming-1.0-SNAPSHOT-jar-with-dependencies.jar producer.KafkaJsonProducer_trx 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
```
sample trx json:
{"timestamp":1565604610745,"shop_id":4,"shop_name":"Ums Eck","cc_type":"Visa","cc_id":"cc_id":"5130-2220-4900-6727","amount_orig":86.82,"fx":"EUR","fx_account":"CHF"}
```  
- KafkaJsonProducer_fx - generating fake Foreign Exchange Rates for some currencies and publishing the JSON string on Kafka / topic = trx 
- run with:  
$ java -classpath streaming-1.0-SNAPSHOT-jar-with-dependencies.jar producer.KafkaJsonProducer_fx  or  
$ java -classpath streaming-1.0-SNAPSHOT-jar-with-dependencies.jar producer.KafkaJsonProducer_fx 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  

```  
sample fx json:
{"timestamp":1565604610729,"fx":"EUR","fx_rate":0.91}
```



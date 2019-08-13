# Streaming

## Concept:  
### Use cases:  
#### Merge two data steams (trx get latest fx rate):  


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


### Test Data Generator:
The project provides two test-data generators:  
- KafkaJsonProducer_trx - generating fake Credit Card Transaction  
```
{"timestamp":1565604389166,"shop_id":0,"shop_name":"Ums Eck","cc_type":"Revolut","cc_id":"5179-5212-9764-8013","amount_orig":75.86,"fx":"CHF","fx_account":"CHF"}
```  
- KafkaJsonProducer_fx - generating fake Foreign Exchange Rates for some currencies  
```  
{"timestamp":1565604494202,"fx":"EUR","fx_rate":1.01}
```

### Test Environment:  
```
cd /Users/mdaeppen/infra/kafka_2.12-2.3.0  
bin/zookeeper-server-start.sh config/zookeeper.properties  
bin/kafka-server-start.sh config/server.properties  


./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic trx &&  
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic fx &&  
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092 &&  
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fx
```

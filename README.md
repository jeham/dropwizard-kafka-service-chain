# dropwizard-kafka-service-chain

## Start Kafka, local installation

    bin\windows\zookeeper-server-start.bat config\zookeeper.properties
    bin\windows\kafka-server-start.bat config\server.properties

## Create topics

    bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic placed-orders
    bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invalid-orders
    bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic valid-orders
    bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic processed-orders

## Start order processing service

    java -jar target\order-processing-service-1.0-SNAPSHOT.jar server config.yml
    
## Start order validation service

    java -jar target\order-validation-service-1.0-SNAPSHOT.jar server config.yml

## Start order placement service

    java -jar target\order-placement-service-1.0-SNAPSHOT.jar server config.yml

## Post a message to the order-placement-service 

    curl -H "Content-Type: application/json" -X POST -d "{ \"orderId\": \"00000000-10d6-44c8-862b-f85d5b9a75ea\", \"customerId\": \"10000000-4f23-4cfd-b852-52fdb802b2df\", \"orderAmount\": 1234567 }" http://localhost:8080/place-order

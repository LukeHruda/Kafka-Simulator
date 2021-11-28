from kafka import KafkaConsumer
from kafka.vendor.six import assertRaisesRegex
consumer = KafkaConsumer()

topics = consumer.topics()

arr = []

for topic in topics:
    print(topic)
    consumer = KafkaConsumer(topic, consumer_timeout_ms=100)
    for message in consumer:
        if message.value is not None:
            print("Here")
            arr.append(consumer)
            break
    

print(len(topics))
print(len(arr))
exit
while 1:
    for consumer in arr:
        #print(consumer.topics(),end="")
        for message in consumer:
            print(message.topic,end ="")
            print (message.value)
            break
from kafka import KafkaConsumer
consumer0 = KafkaConsumer('a0')
consumer1 = KafkaConsumer('a1')
consumer2 = KafkaConsumer('a2')
consumer3 = KafkaConsumer('a3')
consumer4 = KafkaConsumer('a4')
consumer5 = KafkaConsumer('a5')
consumer6 = KafkaConsumer('a6')
consumer7 = KafkaConsumer('a7')
consumer8 = KafkaConsumer('a8')
consumer9 = KafkaConsumer('a8')

arr = [consumer0, consumer1,consumer2,consumer3,consumer4,consumer5,consumer6,consumer7,consumer8,consumer9]

while 1:
    for consumer in arr:
        #print(consumer.topics(),end="")
        for message in consumer:
            print(message.topic,end ="")
            print (message.value)
            break
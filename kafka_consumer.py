from confluent_kafka import Consumer
import json
c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest','enable.auto.commit': 'false'})
c.subscribe(['stream'])

while True:
    print("Listening")
    msg = c.poll(0)

    if msg is None:
        continue
    else :
        print(msg)
        print("Error reading message : {}".format(msg.error()))
        continue
        print(msg)
        c.commit()
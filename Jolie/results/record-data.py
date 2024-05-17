import time
import math
import requests

with open("safe-safe_1.csv", 'a', newline='') as results:
    results.write("RequestId SentTime ProducerRecievedTime ProducerUpdateTime EnteredKafkaTime ExitedKafkaTime ConsumerUpdateTime\n")
    for i in range (0, 1000):
        ms = math.floor(time.time()*1000)
        print("Timer: Sending request {0} at time {1} \n".format(i, str(ms)))
        results.write("{} {}".format(i, str(ms)))
        results.flush()
        r = requests.get('http://localhost:8080/startChoreography?username=user1')
        time.sleep(0.25)
        results.writelines("\n")
import time
import math
import requests

# Clear the file and initialize columns
with open("tempResults.csv", 'w', newline='') as results:
    results.write("RequestId SentTime ProducerRecievedTime ProducerUpdateTime EnteredKafkaTime ExitedKafkaTime ConsumerUpdateTime\n")

# Begin recording data
with open("tempResults.csv", 'a', newline='') as results:
    for i in range (0, 1100):
        ms = math.floor(time.time()*1000)
        print("Timer: Sending request {0} at time {1} \n".format(i, str(ms)))
        results.write("{} {}".format(i, str(ms)))
        results.flush()
        requests.get('http://localhost:8080/startChoreography?username=user1')
        time.sleep(0.4)
        results.writelines("\n")
from time import sleep
from extract.kafkaproducer import produce_messages

# I want to addd live timer how much time it takes to produce the messages

def produce():

    # write timer logic below 
    produce_messages()
    timer= 0
    while True:
        timer += 1
        print(f"Time elapsed: {timer} seconds", end="\r")
        sleep(1)

produce()


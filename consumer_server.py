from kafka import KafkaConsumer
import time
import json

class ConsumerServer(KafkaConsumer):
    
    def __init__(self):
        super().__init__(
                        "com.police.reports",
                        bootstrap_servers="localhost:9092",
                        group_id="reports",
                        auto_offset_reset= "earliest"
                        )
        self.topic = "com.police.reports"
        
    def receive(self):
        
        while True:
            messages = self.poll(timeout_ms=2000)
            
            if messages:
                for key, message_list in messages.items():
                    for m in message_list:
                        print (json.loads(m.value))
            else:
                time.sleep(1)
            
def start_consumer():
    
    server = ConsumerServer()    
    server.receive()
    
if __name__ == "__main__":
    start_consumer()

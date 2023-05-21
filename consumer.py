import requests
import random

class Consumer:
    def __init__(self, brokers: list, consumer_id: str, start_index: int) -> None:
        self.brokers = brokers
        self.index = start_index
        self.consumer_id = consumer_id

    def consume(self) -> dict:
        while True:
            broker = random.choice(self.brokers)
            try:
                response = requests.get(f"{broker}/get", data={"consumer_id": self.consumer_id, "index": str(self.index)})
                data = response.json()
                if response.status_code == 200:
                    self.index = int(list(dict(data).keys())[0]) + 1
                return data
            except requests.exceptions.ConnectionError:
                self.brokers.remove(broker)
            except Exception as ex:
                print(ex)

if __name__ == "__main__":
    import time
    producer = Consumer(["http://127.0.0.1:5001", "http://127.0.0.1:5002"], "justsometest", -1)
    while True:
        print(producer.consume())
        time.sleep(1)

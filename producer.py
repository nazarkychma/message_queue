import requests


class Producer:
    def __init__(self, brokers: list, main_node_url: str) -> None:
        self.brokers = {node:False for node in brokers}
        self.main_node_url = main_node_url
        self.leader = brokers[0]

    def publish(self, message: str) -> None:
        msg = {"message": message}
        while True:
            try:
                response = requests.post(f"{self.leader}/publish", data=msg, timeout=5)
                if response.status_code == 200:
                    self.__set_new_brokers(response.json()["brokers"])
                    break
                if response.status_code == 503:
                    raise requests.exceptions.ConnectionError
            except requests.exceptions.ConnectionError:
                self.reassign_leader()
        
    def reassign_leader(self) -> None:
        try:
            response = requests.get(f"{self.main_node_url}/assign_new_leader")
            if response.status_code == 200:
                self.__set_new_brokers(response.json()["brokers"])
        except:
            ...

    def __set_new_brokers(self, brokers: dict) -> None:
        self.leader = [k for k, v in brokers.items() if v][0]
        self.brokers = brokers

if __name__ == "__main__":
    import time
    producer = Producer(["http://127.0.0.1:5001", "http://127.0.0.1:5002"], "http://127.0.0.1:5000")
    for i in range(100):
        producer.publish(f"test{i}")
        #time.sleep(1)
    print(producer.brokers)

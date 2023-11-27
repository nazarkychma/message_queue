import requests
import random

class Consumer:
    def __init__(self, brokers: list, main_node_url:str, consumer_id: str, start_index: int, login: str, password: str) -> None:
        self.brokers = brokers
        self.main_node_url = main_node_url
        self.index = start_index
        self.consumer_id = consumer_id
        self.login = login
        self.password = password
        self.jwt_token = self.generate_jwt()

    def consume(self) -> dict:
        while True:
            broker = random.choice(self.brokers)
            try:
                headers = {'Authorization': f'Bearer {self.jwt_token}'}
                response = requests.get(f"{broker}/get", data={"consumer_id": self.consumer_id, "index": str(self.index)}, headers=headers)
                data = response.json()
                if response.status_code == 200:
                    self.index = int(list(dict(data).keys())[0]) + 1
                    return data
                if response.status_code == 401:
                    self.jwt_token = self.generate_jwt()
            except requests.exceptions.ConnectionError:
                self.brokers.remove(broker)
            except Exception as ex:
                print(ex)

    def generate_jwt(self) -> None:
        try:
            credentials = {'login': self.login, 'password': self.password}
            response = requests.post(f"{self.main_node_url}/login", json=credentials)
            if response.status_code == 200:
                return response.json()['access_token']
        except:
            ...

if __name__ == "__main__":
    import time
    producer = Consumer(["http://127.0.0.1:5001", "http://127.0.0.1:5002"], "http://127.0.0.1:5000", "justsometest", -1, 'test', 'password')
    print(producer.jwt_token)
    while True:
        print(producer.consume())
        time.sleep(1)

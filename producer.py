import requests


class Producer:
    def __init__(self, brokers: list, main_node_url: str, login: str, password: str) -> None:
        self.brokers = {node:False for node in brokers}
        self.main_node_url = main_node_url
        self.leader = brokers[0]
        self.login = login
        self.password = password
        self.jwt_token = self.generate_jwt()

    def publish(self, message: str) -> None:
        msg = {"message": message}
        while True:
            try:
                headers = {'Authorization': f'Bearer {self.jwt_token}'}
                response = requests.post(f"{self.leader}/publish", data=msg, timeout=5, headers=headers)
                print(response.text)
                if response.status_code == 200:
                    self.__set_new_brokers(response.json()["brokers"])
                    break
                if response.status_code == 401:
                    self.jwt_token = self.generate_jwt()
                if response.status_code == 503:
                    raise requests.exceptions.ConnectionError
            except requests.exceptions.ConnectionError:
                self.reassign_leader()

    def generate_jwt(self) -> None:
        try:
            credentials = {'login': self.login, 'password': self.password}
            response = requests.post(f"{self.main_node_url}/login", json=credentials)
            if response.status_code == 200:
                return response.json()['access_token']
        except:
            ...

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
    producer = Producer(["http://127.0.0.1:5001", "http://127.0.0.1:5002"], "http://127.0.0.1:5000", 'test2', 'password')
    for i in range(100):
        producer.publish(f"test{i}")
    #    #time.sleep(1)
    print(producer.brokers)
    print(producer.jwt_token)

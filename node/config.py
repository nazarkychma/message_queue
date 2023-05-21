import os

class Config:
    def __init__(self) -> None:
        self.main_node_url = os.getenv("MAIN_NODE_URL")
        self.port = os.getenv("PORT")
        self.leader = False
        self.brokers = {}

    def get_leader(self) -> str:
        return [k for k, v in self.brokers.items() if v][0]
        
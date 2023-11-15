

class ZmqInterfaceSpy:
    def __init__(self, *args, **kwargs):
        self.sent_data = []

    def listen(self):
        pass

    def set_kafka_interface(self, kafka_interface):
        pass

    def send_json(self, data):
        self.sent_data.append(data)

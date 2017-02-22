from streamparse.spout import Spout
from pykafka import KafkaClient

class TweetSpout(Spout):
    outputs = ['word']

    def initialize(self, stormconf, context):
        client = KafkaClient(hosts='127.0.0.1:9092')
        self.topic = client.topics[b'tweets']

    def next_tuple(self):
        consumer = self.topic.get_simple_consumer()
        self.logger.info(consumer)
        for message in consumer:
            if message is not None:
                self.emit([message.value])
            else:
                self.emit(['no_message'])
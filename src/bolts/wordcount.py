import os
from collections import Counter
import re
from streamparse import Bolt


class WordCountBolt(Bolt):
    outputs = ['word', 'count']

    def initialize(self, conf, ctx):
        self.counter = Counter()
        self.pid = os.getpid()
        self.total = 0

    def _increment(self, word, inc_by):
        self.counter[word] += inc_by
        self.total += inc_by

    def process(self, tup):
        word = tup.values[0]
        hashtags = re.findall(r"#(\w+)", word)
        for hashtag in hashtags:
            self._increment(hashtag, 1)
            self.logger.info("counted [{:,}] totalwords [pid={}] word={} count={}]".format(self.total,
                                                                        self.pid, hashtag, self.counter[hashtag]))
            self.emit([hashtag, self.counter[hashtag]])

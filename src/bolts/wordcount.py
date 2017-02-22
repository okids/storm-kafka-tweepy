import os
from collections import Counter

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
        self._increment(word, word.count("anies"))
        self.logger.info("counted [{:,}] words [pid={}] word={}]".format(self.total,
                                                                    self.pid, word))
        self.emit([word, self.counter[word]])

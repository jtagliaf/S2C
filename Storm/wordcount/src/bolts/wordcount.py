from __future__ import absolute_import, print_function, unicode_literals

import os
from collections import Counter

from streamparse import Bolt


class WordCountBolt(Bolt):
    def initialize(self, conf, ctx):
        self.counts = Counter()


    def process(self, tup):
        word = tup.values[0]
        self.counts[word] += 1
        self.emit([word, self.counts[word]])
        self.logger.info('%s: %d' % (word, self.counts[word]))



'''
    def initialize(self, conf, ctx):
        self.counter = Counter()
        self.pid = os.getpid()
        self.total = 0

    def _increment(self, word, inc_by):
        self.counter[word] += inc_by
        self.total += inc_by

    def process(self, tup):
        word = tup.values[0]
        self._increment(word, 10 if word == "dog" else 1)
        if self.total % 1000 == 0:
            self.logger.info("counted [{:,}] words [pid={}]".format(self.total,
                                                                    self.pid))
        self.emit([word, self.counter[word]])
'''
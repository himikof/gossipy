# Copyright (C) 2014 Nikita Ofitserov
# Copyright (C) 2011 Johan Rydberg
# Copyright (C) 2010 Bob Potter
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import math


class FailureDetector:

    MAX_INTERVALS = 1000

    def __init__(self):
        self.last_time = None
        self.intervals = []

    def add(self, arrival_time):
        last_time, self.last_time = self.last_time, arrival_time
        if last_time is None:
            i = 0.75
        else:
            i = arrival_time - last_time
        self.intervals.append(i)
        if len(self.intervals) > self.MAX_INTERVALS:
            self.intervals.pop(0)
    
    def failed(self, current_time, phi=8):
        if self.last_time is None:
            return True
        current_interval = current_time - self.last_time
        exp = -1 * current_interval / self.interval_mean()
        if pow(math.e, exp) < 10**(-phi):
            return True
        return False

    def interval_mean(self):
        return sum(self.intervals) / len(self.intervals)

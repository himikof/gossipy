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

from collections.abc import MutableMapping
import asyncio

from gossipy.detector import FailureDetector


class PeerState(MutableMapping):

    def __init__(self, participant, name=None, phi=8, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.participant = participant
        self.name = name
        self.phi = phi
        self.max_version_seen = 0
        self.attrs = {}
        self.detector = FailureDetector()
        self.alive = False
        self.heart_beat_version = 0

    def set_name(self, name):
        self.name = name

    def update_with_delta(self, k, v, n):
        """."""
        # It's possibly to get the same updates more than once if
        # we're gossiping with multiple peers at once ignore them
        if n > self.max_version_seen:
            self.max_version_seen = n
            self.set_key(k, v, n)
            if k == '__heartbeat__':
                self.detector.add(self.loop.time())

    def update_local(self, k, v):
        # This is used when the peerState is owned by this peer
        self.max_version_seen += 1
        self.set_key(k, v, self.max_version_seen)

    def __iter__(self):
        return iter(self.attrs)

    def __len__(self):
        return len(self.attrs)

    def __contains__(self, key):
        return key in self.attrs

    def __setitem__(self, key, value):
        self.update_local(key, value)

    def set(self, key, value):
        self.update_local(key, value)

    def __getitem__(self, key):
        return self.attrs[key][0]

    def __delitem__(self, key):
        raise NotImplementedError()

    def has_key(self, key):
        return key in self.attrs

    def set_key(self, k, v, n):
        self.attrs[k] = (v, n)
        self.participant.value_changed(self, str(k), v)

    def beat_that_heart(self):
        self.heart_beat_version += 1
        self.update_local('__heartbeat__', self.heart_beat_version)

    def deltas_after_version(self, lowest_version):
        """
        Return sorted by version.
        """
        deltas = []
        for key, (value, version) in self.attrs.items():
            if version > lowest_version:
                deltas.append((key, value, version))
        deltas.sort(key=lambda kvv: kvv[2])
        return deltas

    def check_suspected(self):
        if self.detector.failed(self.loop.time(), self.phi):
            self.mark_dead()
            return True
        else:
            self.mark_alive()
            return False

    def mark_alive(self):
        was_alive, self.alive = self.alive, True
        if not was_alive:
            self.participant.peer_alive(self)

    def mark_dead(self):
        if self.alive:
            self.alive = False
            self.participant.peer_dead(self)

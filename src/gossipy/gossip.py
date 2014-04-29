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


import asyncio

import random
import json
import logging
from collections.abc import MutableMapping

from gossipy.state import PeerState
from gossipy.scuttle import Scuttle


def _address_from_peer_name(name):
    address, port = name.split(':', 1)
    return address, int(port)


def _address_to_peer_name(address):
    return '%s:%d' % (address.host, address.port)


class LoopingCall:
    def __init__(self, func, *args, _loop=None, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self._loop = _loop or asyncio.get_event_loop()
        self.running = False
        self.handle = None
        self.interval = None

    def start(self, interval):
        self.interval = interval
        self.running = True
        self._execute()

    def _execute(self):
        if self.running:
            self.func(*self.args, **self.kwargs)
            self.handle = self._loop.call_later(self.interval, self._execute)

    def stop(self):
        self.running = False
        if self.handle is not None:
            self.handle.cancel()
            self.handle = None


class Participant:
    """Base class for participants."""

    def __init__(self):
        self.gossiper = None

    def make_connection(self, gossiper):
        """Attach this participant to a gossiper."""
        self.gossiper = gossiper

    def peer_alive(self, peer_name):
        """Report that there's a peer alive."""

    def peer_dead(self, peer_name):
        """Report that there's a peer dead."""


class Gossiper(asyncio.DatagramProtocol, MutableMapping):

    def __init__(self, participant, address=None, loop=None):
        """Create a new gossiper.

        @param address: Listen address if the gossiper will not be
            bound to a specific listen interface.
        @param address: C{str}
        """
        self.loop = loop or asyncio.get_event_loop()
        self.state = PeerState(participant, loop=self.loop)
        self._states = {}
        self._address = address
        self._scuttle = Scuttle(self._states, self.state)
        self._heart_beat_timer = LoopingCall(self._beat_heart, _loop=self.loop)
        self._gossip_timer = LoopingCall(self._gossip, _loop=self.loop)
        self.participant = participant
        self._seeds = []
        self._transport = None
        self.name = None

    def _setup_state_for_peer(self, peer_name):
        """Setup state for a new peer."""
        self._states[peer_name] = PeerState(
            self.participant, name=peer_name, loop=self.loop)

    def seed(self, seeds):
        """Tell this gossiper that there are gossipers to
        be found at the given endpoints.

        @param seeds: a sequence of C{'ADDRESS:PORT'} strings.
        """
        self._seeds.extend(seeds)
        self._handle_new_peers(seeds)

    def _handle_new_peers(self, names):
        """Set up state for new peers."""
        for peer_name in names:
            if peer_name in self._states:
                continue
            self._setup_state_for_peer(peer_name)

    def _determine_endpoint(self):
        """Determine the IP address of this peer.

        @raises Exception: If it is not impossible to figure out the
            address.
        @return: a C{ADDRESS:PORT} string.
        """
        # Figure our our endpoint:
        address = self._transport.get_extra_info('sockname')
        if not self._address:
            self._address = address
            if self._address == '0.0.0.0':
                raise Exception("address not specified")
        return '%s:%d' % (address[0], address[1])

    def connection_made(self, transport):
        """Start protocol."""
        self._transport = transport
        self.name = self._determine_endpoint()
        self.state.set_name(self.name)
        self._states[self.name] = self.state
        self._heart_beat_timer.start(1)
        self._gossip_timer.start(0.5)
        self.participant.make_connection(self)

    def connection_lost(self, exc):
        """Stop protocol."""
        logging.info("Connection lost")
        self._gossip_timer.stop()
        self._heart_beat_timer.stop()

    def _beat_heart(self):
        """Beat heart of our own state."""
        self.state.beat_that_heart()

    def datagram_received(self, data, address):
        """Handle a received datagram."""
        self._handle_message(json.loads(data.decode()), address)

    def _gossip(self):
        """Initiate a round of gossiping."""
        #logging.debug("Starting some gossip...")
        live_peers = self.live_peers
        dead_peers = self.dead_peers
        if live_peers:
            self._gossip_with_peer(random.choice(live_peers))

        prob = len(dead_peers) / float(len(live_peers) + 1)
        if random.random() < prob:
            self._gossip_with_peer(random.choice(dead_peers))

        for state in self._states.values():
            if state.name != self.name:
                state.check_suspected()

    def _send_to(self, message, address):
        assert isinstance(message, dict)
        self._transport.sendto(json.dumps(message).encode(), address)

    def _gossip_with_peer(self, peer):
        """Send a gossip message to C{peer}."""
        request = {
            'type': 'request', 'digest': self._scuttle.digest()
        }
        self._send_to(request, _address_from_peer_name(peer.name))

    def _handle_message(self, message, address):
        """Handle an incoming message."""
        #logging.debug("Received: {0}".format(message))
        if message['type'] == 'request':
            self._handle_request(message, address)
        elif message['type'] == 'first-response':
            self._handle_first_response(message, address)
        elif message['type'] == 'second-response':
            self._handle_second_response(message, address)

    def _handle_request(self, message, address):
        """Handle an incoming gossip request."""
        deltas, requests, new_peers = self._scuttle.scuttle(
            message['digest'])
        self._handle_new_peers(new_peers)
        response = {
            'type': 'first-response',
            'digest': requests,
            'updates': deltas,
        }
        self._send_to(response, address)

    def _handle_first_response(self, message, address):
        """Handle the response to a request."""
        self._scuttle.update_known_state(message['updates'])
        response = {
            'type': 'second-response',
            'updates': self._scuttle.fetch_deltas(message['digest'])
        }
        self._send_to(response, address)

    def _handle_second_response(self, message, _):
        """Handle the ack of the response."""
        self._scuttle.update_known_state(message['updates'])

    @property
    def live_peers(self):
        """Property for all peers that we know is alive.

        The property holds a sequence L{PeerState}'s.
        """
        return [p for (n, p) in self._states.items()
                if p.alive and n != self.name]

    @property
    def dead_peers(self):
        """Property for all peers that we know is dead.

        The property holds a sequence L{PeerState}'s.
        """
        return [p for (n, p) in self._states.items()
                if not p.alive and n != self.name]

    # dict-like interface:

    def __getitem__(self, key):
        return self.state[key]

    def set(self, key, value):
        self.state[key] = value

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        raise NotImplementedError()

    def __contains__(self, key):
        return key in self.state

    def has_key(self, key):
        return key in self.state

    def __len__(self):
        return len(self.state)

    def __iter__(self):
        return iter(self.state)

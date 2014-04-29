#!/usr/bin/env python
from gossipy.gossip import Gossiper, Participant as _Participant
import logging
import asyncio


CNT = 20
cnt = 0


class Participant(_Participant):

    def __init__(self, name):
        super().__init__()
        self.name = name
        self.loop = asyncio.get_event_loop()

    def make_connection(self, gossiper):
        super().make_connection(gossiper)
        logging.info("Made connection as {}".format(gossiper.name))

    def value_changed(self, peer, key, value):
        if key == '__heartbeat__':
            return
        #if peer != self.name:
        #    logging.debug("{0} saw {1} change {2} to {3}"
        #                  .format(self.name, peer, key, value))
        if key == 'x':
            global cnt
            cnt += 1
            if cnt == CNT:
                logging.debug("Current time: {}".format(self.loop.time()))
                logging.info("Finished")
        if key == '/leader-election/vote':
            try:
                vote = self.gossiper.get('/leader-election/vote')
                # check consensus:
                for peer in self.gossiper.live_peers:
                    v = peer.get('/leader-election/vote')
                    if v != vote:
                        #print "no consensus", peer, "voted on", v, "(i like", vote, ")"
                        return
            except KeyError:
                #print "key error in vote"
                return
            #print "got consensus on votes"
            self.gossiper.set('/leader-election/master', vote)
        elif key == '/leader-election/master':
            try:
                vote = self.gossiper.get('/leader-election/master')
                # check consensus:
                for peer in self.gossiper.live_peers:
                    v = peer.get('/leader-election/master')
                    if v != vote:
                        return
            except KeyError:
                return
            logging.info("{0}: {1} has been elected as the master at {2}"
                         .format(self.name, vote, self.loop.time()))

    def peer_alive(self, peer):
        #logging.debug("{0} thinks {1} is alive".format(self.name, peer.name))
        #print self.name, "thinks", peer.name, "is alive", peer.keys()
        #print peer.get('/leader-election/priority')
        self._start_election()

    _election_timeout = None

    def _vote(self):
        self._election_timeout = None
        suggested_peer = self.gossiper.name
        arrogance = self.gossiper.get('/leader-election/priority')
        for peer in self.gossiper.live_peers:
            p = peer.get('/leader-election/priority')
            if p > arrogance:
                suggested_peer = peer.name
                arrogance = p
        logging.debug("{0} votes for {1}".format(self.name, suggested_peer))
        try:
            current_master = self.gossiper.get('/leader-election/vote')
            if current_master == suggested_peer:
                #print self.name, "no need to update master"
                return
        except KeyError:
            pass
        self.gossiper.set('/leader-election/vote', suggested_peer)

    def _start_election(self):
        if self._election_timeout is not None:
            self._election_timeout.cancel()
        self._election_timeout = self.loop.call_later(5, self._vote)

    def peer_dead(self, peer):
        logging.debug("{0} thinks {1} is dead".format(self.name, peer.name))
        self._start_election()


@asyncio.coroutine
def main():
    loop = asyncio.get_event_loop()
    members = []

    for i in range(0, CNT):
        participant = Participant('127.0.0.1:%d' % (9000+i))
        gossiper = Gossiper(participant, '127.0.0.1', loop=loop)
        gossiper.set('/leader-election/priority', i)
        endpoint = ('127.0.0.1', 9000 + i)
        transport, _ = yield from loop.create_datagram_endpoint(
            lambda: gossiper, local_addr=endpoint)
        members.append((gossiper, transport, participant))

    for i in range(1, CNT):
        members[i][0].seed(['127.0.0.1:9000'])

    seed = members[0][0]

    def prop_test():
        logging.info("Starting prop test at {}".format(loop.time()))
        seed.set('x', 'value')

    def kill_some():
        if len(members) > (CNT / 2):
            #idx = random.randint(0, len(members) - 1)
            idx = len(members) - 1
            _, transport, _ = members.pop(idx)
            logging.info("Killing {}".format(transport.get_extra_info('peername')))
            transport.close()
        #reactor.callLater(5, kill_some)

    seed.set('test', 'value')
    loop.call_later(5, prop_test)
    loop.call_later(30, kill_some)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('asyncio').setLevel(logging.INFO)
    asyncio.async(main())
    asyncio.get_event_loop().run_forever()

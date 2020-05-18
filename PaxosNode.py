from collections import Counter
import time
from functools import total_ordering

@total_ordering
class Num(object):
    def __init__(self,ctr=None,pid=None):
        self.c  = ctr if ctr is not None else 0
        self.id = pid if pid is not None else 0
    def __lt__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return (self.c < other.c) or (self.c == other.c and self.id < other.id)
    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return (self.c == other.c) and (self.id == other.id)
    def __str__(self):
        return '<Num ctr={} , pid={}>'.format(self.c,self.id)
    def __hash__(self):
        return hash((self.c,self.id))
    def __add__(self,n):
        return Num(self.c + n, self.id)

# A Proposal is just a Num and a proposed value. 
@total_ordering
class Proposal(object):
    def __init__(self,n=None,v=None):
        self.n = n if n is not None else Num()
        self.v = v if v is not None else None 
    def __lt__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.n < other.n
    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.n == other.n
    def __str__(self):
        return '<Proposal n={} , v={}>'.format(self.n,self.v)
    def __hash__(self):
        return hash((self.n,self.v))

class PaxosNode(object):
    """docstring for PaxosNode"""
    def __init__(self, nodeid, otherAddrs, sendfn):
        self.send = sendfn

        # Proposer
        self.vdefault = None 
        self.id = nodeid
        self.P_acceptors = [self.id] + otherAddrs
        # must rx > this many responses to my 'prepare reqs' from A's to have buy-in from majority of A's:
        self.MAJORITY = (0+len(self.P_acceptors))/2.0  # (of acceptors) WARNING: assumes self is already in the list of acceptors        
        self.prepare_responses = {}

        # Acceptor
        self.highest_accepted_proposal = None  # Warning: This must be None, not Proposal(); need to remember if an acceptor hasn't heard any proposals yet.
        self.highest_responded_prepreq = Num(ctr=0,pid=self.id)
        self.A_learners = [self.id] + otherAddrs 

        # Learner
        self.L_accepted_values = {}
        self.v = None

    def initiate_paxos(self,v):
        self.P_propose(v)

    def rx(self,d):
        t = d['type']
        if t == 'prepare request':
            self.A_rx_prepare_request(d)
        elif t == 'accept request':
            self.A_rx_accept_request(d)
        elif t == 'prepare response':
            self.P_rx_prepare_response(d)
        elif t == 'decision':
            self.L_rx_decision(d)

    def P_propose(self,v):
        self.vdefault = v
        n = self.highest_responded_prepreq + 1
        self.prepare_responses[n] = {}
        for to in self.P_acceptors:
            d = {'from': self.id,'to': to,'type': 'prepare request','n':n}
            if to == self.id:
                self.A_rx_prepare_request(d)
            else:
                self.send(d)
            
    def P_rx_prepare_response(self,d):
        # Phase 2a. If the proposer receives a response to its prepare requests 
        n = d['n'] 
        self.prepare_responses[n][d['from']] = d  # for this n, remember who voted for what.
        if len(self.prepare_responses[n]) > self.MAJORITY:  # if we get a majority response for this n, good.
            proposals = [r['p'] for r in self.prepare_responses[n].values() if not (r['p'] is None or (r['p'] is not None and r['p'].v is None))]
            if len(proposals) > 0:
                highest_numbered_proposal = max(proposals)
                v = highest_numbered_proposal.v
            else:
                v = self.vdefault
            p = Proposal(n,v)
            for to in self.prepare_responses[n].keys():  # to each Acceptor I've heard from
                r = {'from': self.id,'to': to,
                    'type': 'accept request','p': p}
                if to == self.id:  # my own self-vote
                    self.A_rx_accept_request(r)
                else:
                    self.send(r)

    # Acceptor methods:
    def A_rx_prepare_request(self,d):
        # Phase 1b. If an acceptor receives a prepare request with number n 
        d['n'] > self.highest_accepted_proposal
        self.highest_responded_prepreq = d['n']
        r = {'from': self.id,'to': d['from'],
            'type': 'prepare response',
            'p': self.highest_accepted_proposal,'n': d['n']}
        if r['to'] == self.id:
            # Just call it directly instead of going over the network.
            self.P_rx_prepare_response(r)
        else:
            self.send(r)

    def A_rx_accept_request(self,d):
        # Phase 2b. If an acceptor receives an accept request
        if self.highest_responded_prepreq <= d['p'].n:
            self.highest_accepted_proposal = d['p'] # accept the proposal
            # Phase 3. (Learning a Chosen Value)
            for l in self.A_learners:
                r = {'from': self.id,'to': l,
                    'type': 'decision',
                    'p': self.highest_accepted_proposal}
                if r['to'] == self.id:
                    self.L_rx_decision(r)
                else:
                    self.send(r)

    # Learner methods:
    def L_rx_decision(self,d):
        # Phase 3. (Learning a Chosen Value)
        self.L_accepted_values[d['from']] = d['p'].v
        # If a majority of Acceptors agree, set our value.
        if len(self.L_accepted_values) == 0: return
        v,c = Counter(self.L_accepted_values.values()).most_common(1)[0]
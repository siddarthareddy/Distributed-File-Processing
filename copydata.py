import sys, time, queue, threading, json, pickle
from collections import Counter
import PaxosNode
import network

q = queue.Queue()

def main():
    myAddr      =  (sys.argv[1], int(sys.argv[2]))
    otherAddrs  = [(sys.argv[i],int(sys.argv[i+1])) for i in range(3,len(sys.argv),2)]
    p = PaxosReplicator(myAddr,otherAddrs)
    def parser(b):
        q.put(pickle.loads(b))
        return b'Enqueued msg!'
    threading.Thread(target=network.worker, daemon=True, args=[myAddr,parser]).start()
    p.run()

class LogValue(object):
    def __init__(self,filename,wordcounts):
        self.filename = filename
        self.wordcounts = wordcounts
    def __str__(self):
        # Summarize the string if it's too long.
        x = sorted(self.wordcounts.items())
        if len(x)>10:
            c = x[:3] + ['...'] + x[-3:]
        else:
            c = x
        return '<Logvalue: {}, counts={} >'.format(self.filename,c)
    def __eq__(self,other):
        return isinstance(other, type(self)) and self.filename==other.filename and self.wordcounts==other.wordcounts
    def __hash__(self):
        return hash((self.filename,tuple(sorted(self.wordcounts.items()))))

class PaxosReplicator(object):

    def __init__(self,myAddr,otherAddrs):
        self.running = True
        self.myAddr = myAddr
        self.otherAddrs = otherAddrs
        self.num_elems = 3 # there are 3 entries in our multi-paxos log.

        self.paxosNodes = []
        for i in range(self.num_elems):
            p = PaxosNode(  nodeid=(self.myAddr,i),
                            otherAddrs=[(o,i) for o in otherAddrs],
                            sendfn=lambda d,i=i: network.send(d['to'][0],{'paxos':True,'elem':i,'msg':d}) # [0]: to get the IP addr from the (addr,elementindex) nodeid
                         )
            self.paxosNodes.append(p)

    def get_log(self):
        return [p.v for p in self.paxosNodes]

    def run(self):
        while True:
            d = q.get()
            if 'cmd' in d and d['cmd']=='k': break
            self.rx(d)

    def rx(self,d):
        if 'cmd' in d:
            cmd = d['cmd']
            if cmd=='replicate':
                self.replicate(d['filename'])
            elif cmd=='stop':
                self.stop()
            elif cmd=='resume':
                self.resume()
            elif cmd=='total':
                self.total(d['logpositions'])
            elif cmd=='print':
                self.print()
            else:
                print('No command named "{}" exists'.format(cmd))
        elif 'paxos' in d:
            # A message from some Paxos node. Route to my corresponding Paxos node.
            if self.running:
                self.paxosNodes[d['elem']].rx(d['msg'])

    def replicate(self,f):
        if self.running: 
            print("Reading file {} and replicate its contents across the Paxos Nodes...".format(f))
        else:
            print("I'm not running, try 'resume'")
            return
        if f is None:
            v = None
        else:
            counts = json.load(open(f,'r'))
            v = LogValue(filename=f,wordcounts=counts)  # the value we want to replicate across the other Paxos nodes

        print("Doing Multi-Paxos on LogValue:",v)
        # Multi-paxos: insert v into the next available log entry and simultaneously reach consensus on it with the other Paxos nodes.
        success = False
        for i,p in enumerate(self.paxosNodes): # For each slot in the log,
            if p.v is not None: continue  # skip locations that have already reached consensus 
            attempts = 2
            while p.v is None and attempts > 0:
                p.initiate_paxos(v)  # propose v.
                t0 = time.time()
                while time.time()-t0 < 1:  # for 1 sec, check the msg Q and service paxos msgs from it.
                    while not q.empty():
                        d = q.get()
                        if 'paxos' not in d:
                            q.put(d)
                        else:
                            self.rx(d)
                    time.sleep(.1)
                attempts -= 1
            if p.v == v:
                success = True
                break
            if success: break 
            
        if success:
            print('Current Nodes value {} was accepted in position {} in the log'.format(v,i))
        else:
            print("Current node couldn't get it's value {} in the log".format(v))
            
        print("My local log is currently:")
        self.print()

    def stop(self):  
        print('Disabling paxos...')
        self.running = False

    def resume(self):
        print('Enabling paxos...')
        self.running = True
        print('Proposing a dummy value to get uptodate')
        self.replicate(None)

    def total(self,logpositions):
        log = self.get_log()
        print(sum(sum(log[p].wordcounts.values()) for p in logpositions if p < len(log) and log[p] is not None))

    def print(self):
        '''prints the filenames of all the log objects.'''        
        for i,logval in enumerate(self.get_log()):
            print("\nLOG ENTRY {}: {}".format(i,logval))

if __name__ == '__main__':
    main()
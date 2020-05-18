import sys, json, pickle
from collections import Counter
import network
import os


def main():
    MY_ADDR = (sys.argv[1], int(sys.argv[2]))
    network.worker(MY_ADDR,parser)

def parser(b):
    d = pickle.loads(b)
    filenames = d['filenames']
    reduce(filenames)
    return b'Done!'

def reduce(fs):
    print("Reducing:",fs)
    counts = Counter()
    for f in fs:
        words = [tup[0] for tup in json.load(open(f,'r'))]  # Turn to list of just words.
        counts.update(Counter(words))
    path,fname = os.path.split(fs[0])
    prefix = fname.split('_')[0]
    outname = prefix+'_reduced'
    out = os.path.join(path,outname)
    json.dump(counts,open(out,'w'))
    print('Reduced to',out)

if __name__ == '__main__':
    main()    
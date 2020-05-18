import sys, pickle, json
from collections import Counter
import network
import os

def main():
    ME = sys.argv[1]  
    MY_ADDR = (sys.argv[2], int(sys.argv[3]))
    
    def parser(b):
        d = pickle.loads(b)
        filename = d['filename']
        offset = d['offset']
        size = d['size']
        Map(ME,filename,offset,size)
        return b'Done!'
    network.worker(MY_ADDR,parser)

def Map(mapid,filename,offset,size):
    print('Mapping file {}, offset={}, size={}...'.format(filename,offset,size))
    s = open(filename,'r').read(offset+size)[offset:]
    words = [w.strip() for w in s.split()]
    counts = [(w,1) for w in words]
    path,inname = os.path.split(filename)
    outname = '{}_I_{}'.format(inname,mapid)
    out = os.path.join(path,outname)
    json.dump(counts,open(out,'w'))
    print('Mapped to file',out)

if __name__ == '__main__':
    main()
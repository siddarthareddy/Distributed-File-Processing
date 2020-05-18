import socket, pickle, time, struct

def send(addr,obj,replyfn=None):
    n = 0
    while True:
        n += 1
        s = socket.create_connection(addr)
        break
    b = pickle.dumps(obj)
    msg = b
    s.sendall(msg)

def worker(ADDR,f):
    print('Hi from Worker on addr',ADDR)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    sock.bind(ADDR)
    sock.listen(10) # max # queued connections
    while True:
        strm,addr = sock.accept() # blocks
        b = b''
        while True:
            tmp = strm.recv(2048) 
            if tmp == b'': break;
            else: b += tmp
        replymsg = f(b)
        if replymsg is not None:           
            strm.sendall(replymsg)
    print('killing...')
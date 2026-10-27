"""Task-local source ports avoid this host's 500-port automatic TCP range.

No OS configuration changes, address reuse, connection retry or POST retry.
Occupied ports are never taken. Only a successful local bind precedes one connect.
"""
import http.client
import socket
import threading

_mutex=threading.Lock()
_next_port=20000

def bound_socket():
    global _next_port
    with _mutex:
        for _ in range(25000):
            port=_next_port
            _next_port=20000+(_next_port-20000+1)%25000
            sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            # Windows exclusive ownership: never reuse or displace another socket.
            if hasattr(socket,'SO_EXCLUSIVEADDRUSE'):
                sock.setsockopt(socket.SOL_SOCKET,socket.SO_EXCLUSIVEADDRUSE,1)
            try:
                sock.bind(('',port))
                return sock
            except OSError:
                sock.close()
        raise OSError('No unused task-local source port; no connect or POST attempted')

class TaskBoundHTTPConnection(http.client.HTTPConnection):
    def connect(self):
        if self._tunnel_host:
            raise ValueError('No proxy/tunnel in the frozen direct transport')
        self.sock=bound_socket()
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host,self.port))

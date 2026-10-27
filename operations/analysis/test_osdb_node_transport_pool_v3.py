"""Synthetic connection pooling and unknown failure checks, no network/model."""
import subprocess,threading,unittest
from concurrent.futures import ThreadPoolExecutor
from osdb_node_transport_pool_v3 import PersistentSSH


class Channel:
    def exec_command(self,c):self.command=c
    def sendall(self,b):self.data=b
    def shutdown_write(self):pass
    def recv_ready(self):return not hasattr(self,'read')
    def recv(self,n):self.read=True;return self.data
    def recv_stderr_ready(self):return False
    def exit_status_ready(self):return hasattr(self,'read')
    def recv_exit_status(self):return 0
    def close(self):pass


class Client:
    made=[]
    def __init__(self):self.connected=False;self.opens=0;self.made.append(self)
    def load_host_keys(self,p):pass
    def set_missing_host_key_policy(self,p):pass
    def connect(self,*a,**kw):self.connected=True
    def get_transport(self):return self
    def set_keepalive(self,n):pass
    def open_session(self,timeout):self.opens+=1;return Channel()
    def is_active(self):return True
    def close(self):self.connected=False


class PoolTests(unittest.TestCase):
    def test_twelve_requests_reuse_two_authentications(self):
        Client.made=[];p=PersistentSSH('unused','unused',client_factory=Client)
        try:
            with ThreadPoolExecutor(max_workers=12) as e:
                values=list(e.map(lambda i:p(['ssh','fixed'],input=str(i).encode(),capture_output=True,timeout=1).stdout,range(12)))
            self.assertEqual(len(Client.made),2);self.assertEqual(sum(c.opens for c in Client.made),12)
            self.assertEqual(values,[str(i).encode() for i in range(12)])
        finally:p.close()
    def test_channel_error_is_unknown_and_never_reconnected(self):
        class Broken(Client):
            def open_session(self,timeout):raise ConnectionError('synthetic')
        Client.made=[];p=PersistentSSH('unused','unused',client_factory=Broken)
        try:
            with self.assertRaises(subprocess.TimeoutExpired):p(['ssh','fixed'],input=b'x',capture_output=True,timeout=1)
            self.assertEqual(len(Client.made),2)
        finally:p.close()


if __name__=='__main__':unittest.main()

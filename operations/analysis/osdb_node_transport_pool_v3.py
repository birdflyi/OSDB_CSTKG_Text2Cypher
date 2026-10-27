"""Two authenticated SSH connections, six channels each; no request retries.

Preserves the accepted node one-shot worker and receipt/unknown semantics.
Connection setup is sequential, before any model reservation. Host keys required.
"""
import queue,subprocess,time
from pathlib import Path
import paramiko
from osdb_node_transport_live import NodeTransport


class PersistentSSH:
    def __init__(self,key,known_hosts,*,connections=2,channels=6,client_factory=paramiko.SSHClient):
        if connections!=2 or channels!=6:raise ValueError('Fixed 2x6 development transport')
        self.clients=[];self.available=queue.Queue()
        try:
            for _ in range(connections):
                client=client_factory();self.clients.append(client)
                client.load_host_keys(str(known_hosts));client.set_missing_host_key_policy(paramiko.RejectPolicy())
                client.connect('117.50.195.94',port=3107,username='root',key_filename=str(key),
                    look_for_keys=False,allow_agent=False,timeout=10,auth_timeout=15,banner_timeout=15)
                client.get_transport().set_keepalive(20)
                for _ in range(channels):self.available.put(client)
        except BaseException:self.close();raise
    def __call__(self,args,*,input,capture_output,timeout):
        client=None;channel=None;start=time.monotonic()
        try:
            client=self.available.get(timeout=10)
            channel=client.get_transport().open_session(timeout=10)
            channel.exec_command(args[-1]);channel.sendall(input);channel.shutdown_write()
            output=bytearray();errors=bytearray()
            while True:
                if time.monotonic()-start>=timeout:raise TimeoutError('SSH channel deadline')
                while channel.recv_ready():output.extend(channel.recv(65536))
                while channel.recv_stderr_ready():errors.extend(channel.recv_stderr(65536))
                if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
                    return subprocess.CompletedProcess(args,channel.recv_exit_status(),bytes(output),bytes(errors))
                if not client.get_transport().is_active():raise ConnectionError('Persistent SSH closed')
                time.sleep(.01)
        except Exception as exc:
            # NodeTransport marks this attempt unknown and stops; never reconnect.
            raise subprocess.TimeoutExpired(args,timeout) from exc
        finally:
            if channel is not None:channel.close()
            if client is not None:self.available.put(client)
    def close(self):
        for client in self.clients:client.close()


class PooledNodeTransport(NodeTransport):
    def __init__(self,ssh,remote_root,initial,evidence):
        self.pool=PersistentSSH('C:/Users/10651/.codex/private/ch7_ssh_node3107_20261026/interactive_3107.key',
            Path.home()/'.ssh/known_hosts')
        super().__init__(ssh,remote_root,initial,evidence,run=self.pool)
    def close(self):self.pool.close()

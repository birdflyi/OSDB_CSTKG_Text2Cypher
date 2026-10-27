"""64-request transport: 8 authenticated connections x 8 session channels.

Only the explicitly requested new capacity test uses this class. No reconnect,
host-key relaxation, sshd changes, old campaign resume or cap above64.
"""
import hashlib,queue
from pathlib import Path
import paramiko
from osdb_node_transport_pool_v3 import PersistentSSH
from osdb_node_transport_live import NodeTransport
from osdb_scoped_attempts_dev import wire

class PersistentSSH64(PersistentSSH):
 def __init__(self,key,known_hosts,client_factory=paramiko.SSHClient):
  self.clients=[];self.available=queue.Queue()
  try:
   for _ in range(8):
    client=client_factory();self.clients.append(client)
    client.load_host_keys(str(known_hosts));client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect('117.50.195.94',port=3107,username='root',key_filename=str(key),look_for_keys=False,
      allow_agent=False,timeout=10,auth_timeout=15,banner_timeout=15)
    client.get_transport().set_keepalive(20)
    for _ in range(8):self.available.put(client)
  except BaseException:self.close();raise

class CapacityNodeTransport(NodeTransport):
 def __init__(self,ssh,remote_root,initial,evidence,pool):
  self.ssh=list(ssh);self.remote_root=remote_root;self.evidence=Path(evidence);self.run=pool
  self.identities={hashlib.sha256(wire(messages)).hexdigest():key for key,messages in initial.items()}
  if len(self.identities)!=64:raise ValueError('Exactly64 new sealed capacity requests required')

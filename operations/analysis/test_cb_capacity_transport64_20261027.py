"""No-network check for64 channel tokens and exactly8 authenticated clients."""
import unittest
from concurrent.futures import ThreadPoolExecutor
from test_osdb_node_transport_pool_v3 import Client
from cb_capacity_transport64_20261027 import PersistentSSH64

class Capacity64Tests(unittest.TestCase):
 def test_sixty_four_channels_eight_authentications(self):
  Client.made=[];p=PersistentSSH64('unused','unused',client_factory=Client)
  try:
   self.assertEqual(p.available.qsize(),64)
   with ThreadPoolExecutor(max_workers=64) as pool:
    got=list(pool.map(lambda i:p(['ssh','fixed'],input=str(i).encode(),capture_output=True,timeout=1).stdout,range(64)))
   self.assertEqual(len(Client.made),8);self.assertEqual(sum(c.opens for c in Client.made),64)
   self.assertEqual(got,[str(i).encode() for i in range(64)])
  finally:p.close()

if __name__=='__main__':unittest.main()

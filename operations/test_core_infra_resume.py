import io
from pathlib import Path
from types import SimpleNamespace
import unittest

from core_infra_resume import keepalive_command, safe_close


class InfrastructureOnlyChecks(unittest.TestCase):
    def test_preserves_exact_remote_command_and_non_ssh_processes(self):
        command=['ssh','-p','2491','root@host','python','worker.py','--graph','example']
        amended=keepalive_command(command)
        self.assertEqual(amended[5:],command[1:])
        self.assertEqual(command[1],'-p')
        self.assertEqual(keepalive_command(['python','script.py']),['python','script.py'])

    def test_broken_pipe_still_waits_and_closes_log(self):
        class BrokenStdin:
            closed=False
            def close(self):
                raise OSError('broken pipe')
        class Process:
            stdin=BrokenStdin()
            waited=False
            def wait(self,timeout):
                self.waited=True
        process=Process();log=io.StringIO()
        safe_close(SimpleNamespace(process=process,log=log))
        self.assertTrue(process.waited)
        self.assertTrue(log.closed)

"""Native query routing is opt-in from the existing live CLI."""
from contextlib import redirect_stdout
from contextlib import redirect_stderr
import io
import unittest

from ops.warehouse_live import main


class NativeLiveCliTests(unittest.TestCase):
    def test_existing_live_help_lists_published_point_option(self):
        output=io.StringIO()
        with redirect_stdout(output),self.assertRaises(SystemExit) as exited:main(['--help'])
        self.assertEqual(exited.exception.code,0)
        self.assertIn('--query-point-file',output.getvalue())
        self.assertIn('--query-upstream',output.getvalue())
        self.assertIn('--max-active-robots',output.getvalue())

    def test_active_limit_rejects_invalid_arguments_before_startup(self):
        for value in ('0','-1','101','50.0','abc'):
            with self.subTest(value=value),redirect_stderr(io.StringIO()),self.assertRaises(SystemExit) as exited:
                main(['--max-active-robots',value])
            self.assertEqual(exited.exception.code,2)


if __name__=='__main__':unittest.main()

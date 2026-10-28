"""Native query routing is opt-in from the existing live CLI."""
from contextlib import redirect_stdout
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


if __name__=='__main__':unittest.main()

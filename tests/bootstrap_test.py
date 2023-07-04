import unittest
from metadatum.bootstrap import Bootstrap

class BootstrapTestCase(unittest.TestCase):

    def setUp(self):
        self.bootstrap = Bootstrap()

    def test_run(self):
        """Test run"""

        # run return None
        result = self.bootstrap.run()
        self.assertEqual(result, None)
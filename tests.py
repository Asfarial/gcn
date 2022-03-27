"""
Unittest Module
"""
import unittest
from reader import Reader


class Discount(unittest.TestCase):
    """
    Testing Reader().discount()
    """

    @classmethod
    def setUpClass(cls) -> None:
        """
        Printing name of cls
        :return:
        """
        print(f"Test Case: {cls.__name__}")

    def setUp(self) -> None:
        """
        Calling New Instance Each Test
        :return:
        """
        self.reader = Reader()

    def test_empty(self):
        """Tests if price and old price not provided"""
        line = {"price": "", "old_price": ""}
        discount = self.reader.discount(line)
        self.assertEqual(discount, "")

    def test_price_only(self):
        """Tests If Price provided only"""
        line = {"price": "100", "old_price": ""}
        discount = self.reader.discount(line)
        self.assertEqual(discount, "")

    def test_old_price_only(self):
        """Tests If Old Price provided only"""
        line = {"price": "", "old_price": "100"}
        discount = self.reader.discount(line)
        self.assertEqual(discount, "")

    def test_price_gt_old_price(self):
        """Tests If Price > Old Price"""
        line = {"price": "150", "old_price": "100"}
        discount = self.reader.discount(line)
        self.assertEqual(discount, "")

    def test_price_e_old_price(self):
        """Tests If Price == Old Price"""
        line = {"price": "100", "old_price": "100"}
        discount = self.reader.discount(line)
        self.assertEqual(discount, "")

    def test_price_lt_old_price(self):
        """Tests If Price < Old Price"""
        line = {"price": "70", "old_price": "100"}
        discount = self.reader.discount(line)
        self.assertEqual(discount, "30%")

    def test_float(self):
        """Tests How Treats Float in Str"""
        line = {"price": "70.0", "old_price": "100.0"}
        discount = self.reader.discount(line)
        self.assertEqual(discount, "30%")


if __name__ == "__main__":
    unittest.main()

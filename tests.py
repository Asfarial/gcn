import unittest
import main

class Discount(unittest.TestCase):
    def setUp(self) -> None:
        self.reader = main.Reader()

    def test_empty(self):
        line = {'price':'', 'old_price':''}
        discount = self.reader.discount(line)
        self.assertEqual(discount, '')

    def test_price_only(self):
        line = {'price':'100', 'old_price':''}
        discount = self.reader.discount(line)
        self.assertEqual(discount, '')

    def test_old_price_only(self):
        line = {'price':'', 'old_price':'100'}
        discount = self.reader.discount(line)
        self.assertEqual(discount, '')

    def test_price_gt_old_price(self):
        line = {'price':'150', 'old_price':'100'}
        discount = self.reader.discount(line)
        self.assertEqual(discount, '')

    def test_price_e_old_price(self):
        line = {'price':'100', 'old_price':'100'}
        discount = self.reader.discount(line)
        self.assertEqual(discount, '')

    def test_price_lt_old_price(self):
        line = {'price':'70', 'old_price':'100'}
        discount = self.reader.discount(line)
        self.assertEqual(discount, '30%')

    def test_float(self):
        line = {'price':'70.0', 'old_price':'100.0'}
        discount = self.reader.discount(line)
        self.assertEqual(discount, '30%')

if __name__ == '__main__':
    unittest.main()

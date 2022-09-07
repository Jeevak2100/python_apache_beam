import unittest
import apache_beam as beam

from Task2 import MyTransform


class TestPower(unittest.TestCase):

    def my_unit_Test(self):
        print("Unit Test 2")
        self.assertEqual(1, 1)

    def test_power_int(self):
        print("Unit Test 1")
        pcoll = beam.Pipeline()
        at = (
                pcoll
                | beam.io.ReadFromText('beam_input.csv')
                | MyTransform()
        )

        self.assertEqual(pcoll, 1)


if __name__ == '__main__':
    unittest.main()

import unittest

from SQLparser import execSQL, getTableOid

class TestSQL(unittest.TestCase):
    def test_execSQL(self):
        expectedOutput = """ ?column? | ?column? 
----------+----------
        1 |        2
(1 row)
"""
        assert (expectedOutput.strip()==execSQL("select 1,2;").strip())

    def test_getTblOID(self):
        assert(isinstance(getTableOid("pg_stats"),int))

if __name__ == '__main__':
    unittest.main()

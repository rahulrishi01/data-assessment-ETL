import unittest
from application import parse_xml

def test_parse_xml():
    print("===========================")
    test_file_name = 'GL_AK_2020-11-24.xml'
    df = parse_xml(test_file_name)
    assert len(df) >= 0
'''Main File to run'''
import time
import json
from datetime import datetime
import xmltodict
import pandas as pd
from pandas import json_normalize
import config
import utils


logger_name = 'data_assessment_etl_logger'
logger = utils.establish_logger(logger_name=logger_name)


def parse_xml(xml_file):
    """
        Parse nested xml file and build a dataframe
    :param xml_file: input xml file
    :return: parsed dataframe
    """
    try:
        with open(xml_file) as xml_file:
            data_dict = xmltodict.parse(xml_file.read())
        xml_file.close()
        header = data_dict['Ledger']['Header']
        transactions = data_dict['Ledger']['Transactions']
        df = pd.DataFrame()
        for t in transactions['Transaction']:
            df1 = pd.DataFrame()
            df1.loc[0, 'JournalCode'] = t.get('JournalCode', None)
            df1['JournalDescription'] = t.get('JournalDescription', None)
            df1['AccountEvent'] = t.get('AccountEvent', None)
            df1['AccountType'] = t.get('AccountType', None)
            df1['DepartureStation'] = t.get('DepartureStation', None)
            df1['TailNumber'] = t.get('TailNumber', None)
            df1['RouteAndFlightNumber'] = t.get('RouteAndFlightNumber', None)
            df1['ReferenceCode'] = t.get('ReferenceCode', None)
            df1['merge_id'] = 1
            if type(t['AccountInfoLists']['AccountInfoList']) == list:
                temp = json_normalize(t['AccountInfoLists'], record_path=['AccountInfoList'], errors="ignore")
            else:
                temp = json_normalize(t['AccountInfoLists']['AccountInfoList'])
            temp['merge_id'] = 1
            df = pd.concat([df, pd.merge(df1, temp, on='merge_id')])
            df.insert(0, 'CompanyCode', header['CompanyCode'])
            df.insert(1, 'AccountingDate', header['AccountingDate'])
            return df
    except Exception as e:
        logger.exception("exception", extra={'exp_msg': e})


if __name__ == "__main__":
    """
        Starting point of the program
    """
    try:
        file_name = 'GL_AK_2020-11-24.xml'
        df = parse_xml(file_name)
        print(df)
    except Exception as e:
        logger.exception("exception", extra={'exp_msg': e})
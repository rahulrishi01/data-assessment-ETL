import pandas as pd
import pymysql
import pytz
from datetime import datetime
import time
import logging
import sys
import config
from sqlalchemy import create_engine
import logging.handlers
from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(
            log_record, record, message_dict)

        log_record['event_time'] = log_record['asctime']
        log_record['logger_name'] = log_record['name']
        log_record['log_level'] = log_record['levelname']
        log_record['function_name'] = log_record['funcName']
        del log_record['asctime'], log_record['name'], log_record['levelname'], log_record['funcName']


def establish_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.CRITICAL)
    logger.setLevel(logging.ERROR)
    logger.setLevel(logging.WARNING)
    logger.setLevel(logging.INFO)

    # Handler
    # handler = logging.handlers.RotatingFileHandler(
    # file_path, maxBytes=1048576, backupCount=5)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.CRITICAL)
    handler.setLevel(logging.ERROR)
    handler.setLevel(logging.WARNING)
    handler.setLevel(logging.INFO)

    formatter = CustomJsonFormatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s')

    # Add Formatter to Handler
    handler.setFormatter(formatter)

    # add Handler to Logger
    logger.addHandler(handler)
    return logger


def now_utc():
    return datetime.now(pytz.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")


def put_sql_data(dfs, schema, tab_nm, logger):
    """
        :@param dfs: data to store
        :@param schema: schema name in which data will be stored
        :@param tab_nm: table name in which data will be stored
        :@param logger: logger object
        :return: NA
        """
    try:
        start_time = time.time()
        logger.info("entry", extra={'time': start_time})
        put_sql_conn_string = "mysql+pymysql://" + config.db_user_name + ":" + config.db_user_pass + "@" + \
                              config.db_host_name + ":" + str(config.db_host_port) + "/" + schema
        db_conn = create_engine(put_sql_conn_string)
        dfs.to_sql(name=tab_nm, con=db_conn, if_exists='append', index=False)
        logger.info("db put complete, exit", extra={'time': time.time(),
                                                    'compute_time': round(time.time() - start_time, 4)})

    except Exception as e:
        logger.exception("exception", extra={'tbl_name':tab_nm, 'df':dfs,
                                             'exp_loc':'exit','exp_msg': e})
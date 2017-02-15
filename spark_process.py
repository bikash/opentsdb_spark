"""
Concept from: 
[1] https://github.com/venidera/otsdb_client/blob/master/otsdb_client/client.py
2017.02, Bikash Agrawal, 
Insert spark dataframe to opentsdb 
Example usage
--------------
# /raid5/Spark/spark-2.0.2-bin-hadoop2.7/bin/pyspark --packages com.databricks:spark-csv_2.11:1.5.0 --jars /home/bikaa/repos/opentsdb_spark/lib/spark-csv_2.11-1.5.0.jar spark_process.py
#$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-csv_2.11:1.5.0
>>> import opentsdb
>>> oc = opentsdb()
>>> oc.ts_insert(df)
Note: it works only with spark dataframe. 
"""

import requests as gr
import time
import itertools
from datetime import datetime
import socket
import urllib2
import httplib
import json
import datetime as dt
import random
from dateutil import rrule
from collections import OrderedDict
from multiprocessing import Process, Queue, Pool
import time
import logging
import logging.config
import re
from json import dumps as tdumps, loads
import sys, os

from pyspark.sql.types import *


logging.basicConfig(level=logging.WARNING)

logger = logging.getLogger('opentsdb.process')



class spark_opentsdb(object):
	def __init__(self, hostname='localhost', port=4242):
        self.hostname = hostname
        self.port = port
        self.sc = None
        self.spark = None

    def spark_init():
    	from pyspark import SparkContext
		from pyspark.sql import SQLContext
		self.sc = SparkContext(appName="OpentSDB data processing")
		from pyspark.sql import SparkSession
		self.spark = SparkSession(sc)

    def get_input(self, start_date, end_date, metric, tags):
    	import opentsdb
    	oc = opentsdb(self.hostname = hostname,self.port)
    	oc.put()

    def put_data(self, csv):

		df = self.spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(csv)
		df.show()




if __name__ == "__main__":
    spark = spark_opentsdb("osl5305",9998)
    spark.spark_init()
	spark.put_data()




# coding: utf-8

# In[1]:

#!/usr/bin/env python
__author__ = "Bryan Yang"
__version__ = "1.0.1"
__maintainer__ = "Bryan Yang"


# In[ ]:

from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
from datetime import datetime
import os
import gc
import sys
import time


# In[54]:

def run(inpath, outpath):
    
    gc.disable()
    # initial SparkContext
    conf = SparkConf().setAppName("Forgate Log Parser")
    sc = SparkContext(conf=conf)
    sqlCtx = HiveContext(sc)
    start_time = time.time()
    print("===== INPUT FILE PATH: %s =====" % (str(inpath)))
    print("===== OUTPUT FILE PATH: %s =====" % (str(outpath)))
    
    print("===== %s Reading Data From HDFS" % (now()))
    distFile = sc.textFile(inpath)
    cnt_raw = distFile.count()
    print("===== Count of Input Data: %s =====" % (str(cnt_raw)))
    
    print("===== %s Parsing Data" % (now()))
    parsedData = parse_data(sc, distFile)
    print("===== Count of Parsed Data: %s =====" % (str(parsedData.count())))
    
    print("===== %s Saving Data" % (now()))
    jsonData = sqlCtx.jsonRDD(parsedData)
    jsonData.write.partitionBy('date').parquet(outpath,mode='overwrite')
    
    print("===== %s Checking Data" % (now()))
    cnt_parquet = confirm_row(sqlCtx, outpath)
    print("===== Count of Parquet Data: %s =====" % (str(cnt_parquet)))
    
    if (cnt_raw == cnt_parquet):
        print("===== Pass =====")
    else:
        print("===== Not Pass =====")
    
    print("---Total %s seconds ---" % (time.time() - start_time))
    
    sc.stop()
    gc.enable()
    


# In[42]:

def parse_data(sc, df):
    parsedData = df.map(lambda x: _space_split(x))                .map(lambda x: [x[:4],x[4:]])                .map(lambda x: dict([('month',x[0][0].encode('ascii', 'ignore')),                ('day',x[0][1].encode('ascii', 'ignore')),                ('time',x[0][2].encode('ascii', 'ignore')),                ('ip',x[0][3].encode('ascii', 'ignore'))] +     [(i[0].encode('ascii', 'ignore'),i[1].encode('ascii', 'ignore')) for i in [i.split('=') for i in x[1]] if len(i)==2]))
    return parsedData
            


# In[2]:

def confirm_row(sqlCtx, outpath):
    df = sqlCtx.read.parquet(os.path.join(outpath))
    cnt = df.count()
    return cnt


# In[3]:

def now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# In[44]:

def _space_split(string):
        """
        There are some blank in single/ double quotes in the data(like country)
        Avoid to split it.
        :param string: string
        :return: list
        """
        last = 0
        splits = []
        inQuote = None

        for i, letter in enumerate(string):
            if inQuote:
                if (letter == inQuote):
                    inQuote = None

            else:
                if (letter == '"' or letter == "'"):
                    inQuote = letter

            if not inQuote and letter == ' ':
                splits.append(string[last:i])
                last = i + 1

        if last < len(string):
            splits.append(string[last:])

        return splits


# In[23]:

if __name__ == '__main__':
    # arguments
    if len(sys.argv) == 3:
        args = sys.argv
    else:
        raise ValueError("logparser_spark.py [hdfs path if input file] [hdfs path of output file]")

    run(args[1], args[2])


# In[ ]:




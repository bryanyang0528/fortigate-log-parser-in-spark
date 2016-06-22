
# coding: utf-8

# In[34]:

#!/usr/bin/env python
from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
from datetime import datetime
import os
import gc
import sys
import time

__author__ = "Bryan Yang"
__version__ = "1.0.1"
__maintainer__ = "Bryan Yang"
__home__="/home/bryan.yang/fortigate/src/logparser_spark"
__path_of_log__= os.path.join(__home__,"log_history.log")


# In[ ]:

def run(inpath, outpath):
    
    gc.disable()
    print("===== Checking if Log Exists =====")
    check_log(inpath)
    print("===== Pass Log Checking =====")
    
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
    old_col=['time','date']
    new_col=['time_','dt']
    jsonData = rename_column(jsonData, old_col, new_col)
    jsonData.write.partitionBy('dt').parquet(outpath,mode='append')
    
    print("===== %s Checking Data" % (now()))
    confirm_row(sqlCtx, outpath)
    write_log(inpath)
    print("---Total took %s seconds ---" % (time.time() - start_time))
    
    sc.stop()
    gc.enable()
    


# In[ ]:

def parse_data(sc, df):
    parsedData = df.map(lambda x: _space_split(x))                .map(lambda x: [x[:4],x[4:]])                .map(lambda x: dict([('month',x[0][0].encode('ascii', 'ignore')),                ('day',x[0][1].encode('ascii', 'ignore')),                ('time',x[0][2].encode('ascii', 'ignore')),                ('ip',x[0][3].encode('ascii', 'ignore'))] +     [(i[0].encode('ascii', 'ignore'),i[1].encode('ascii', 'ignore')) for i in [i.split('=') for i in x[1]] if len(i)==2]))
    return parsedData
            


# In[ ]:

def confirm_row(sqlCtx, outpath):
    df = sqlCtx.read.parquet(outpath)
    df.groupBy('dt').count().show()


# In[22]:

def now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# In[4]:

def rename_column(df, old_col, new_col):
    if isinstance(old_col, basestring) and isinstance(new_col, basestring):
        df = df.withColumnRenamed(old_col, new_col)
    elif isinstance(old_col, list) and len(old_col) == len(new_col):
        for i in xrange(len(old_col)):
            df = df.withColumnRenamed(old_col[i], new_col[i])
    else:
        raise ValueErrorr("length of old and new column name is not match")
    return df


# In[63]:

def check_log(inpath):
    #check if a file exists and create it
    open(__path_of_log__, "a")
    log_name = inpath.split("/")[-1]
    with open(__path_of_log__) as f:
        lines = f.read().splitlines()
        logs = [i.split(" ")[-1] for i in lines]
    if log_name in logs:
        raise ValueError("the log file has been loaded")
    else:
        return True


# In[46]:

def write_log(inpath):
    log_name = inpath.split("/")[-1]
    with open(__path_of_log__, "a") as f:
        f.write("%s %s\n" % (now(),log_name))


# In[ ]:

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


# In[ ]:

if __name__ == '__main__':
    # arguments
    if len(sys.argv) == 3:
        args = sys.argv
    else:
        raise ValueError("logparser_spark.py [hdfs path if input file] [hdfs path of output file]")
    # check if log has been loaded
    
    run(args[1], args[2])


# In[3]:




# In[ ]:




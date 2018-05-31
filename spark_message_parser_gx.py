import re 
import sys
import subprocess
from pyspark import SparkContext, SparkConf
from graphframes import *
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql import Row

#Relevant message fields
#Get a record from a raw email 

_msgId = "Message-ID"
_date = "Date"
_from = "From"
_to = "To"
_cc = "Cc"
_bcc = "Bcc"
_xTo = "X-To"
_xCc = "X-Cc"
_xBcc = "X-Bcc"
_subject = "Subject"
_body = "Body"


endOfHeader = ""
email_seprator = ','

ctrl_list = [_msgId, _date, _from, _to, _cc, _bcc, _xTo, _xCc, _xBcc, _subject, _body] 

#file to read
#path = "/user/pnda/enron/enron/maildir/allen-p/inbox/"
path = "/user/pnda/enron-flat"
test_path = '/user/pnda/enron/enron/maildir/allen-p/inbox/1.,/user/pnda/enron/enron/maildir/allen-p/inbox/10.'
cr = '\n'


def get_vertex_list(meta):
    return parser(meta, 2)


def get_edge_list(meta):
    return parser(meta, 1)


def parser(meta, case):
    name, rawMessage = meta
    rawMessage = rawMessage.split(cr)
    length = len(rawMessage)
    index = 0
    msg_Id = ''
    to_data = []
    from_data = []

    for line in rawMessage:
        line = line.strip()
        index = index + 1
        if line == '':
            body_data = ''.join(rawMessage[index:])
            break
        if line.startswith(_msgId):
            offset = get_next_ctrl_offset(index, rawMessage)
            msgId = line.split(':')[1] + ''.join(rawMessage[index: offset - 1])
        if line.startswith(_to):
            first_list = line.split(':')[1]
            to_data = get_email_list(first_list)
            offset = get_next_ctrl_offset(index, rawMessage)
            for xindex in range(index, offset - 1):
                data = get_email_list(rawMessage[xindex])
                to_data = to_data + data
        if line.startswith(_from):
            first_list = line.split(':')[1]
            xfrom_data = get_email_list(first_list)
            if xfrom_data: 
                from_data = xfrom_data[0]
            else:
                from_data = "dummy"
    #return [(from_data , len(to_data))] 
    if case == 1: 
        each_record = [] 
        for to_ in to_data:
            item = (from_data, to_, 1) 
            each_record.append(item)
        return each_record
    else:
        return [(from_data)]
    #return [(from_data , to_data)] 


def get_input(path):
    result = subprocess.check_output(['hadoop', 'dfs', '-lsr', path])
    file_list = []
    result_list = result.split('\n')
    for meta in result_list:
        path = meta.split(" ")[-1]
        if path.endswith('.'):
            file_list.append(path)
    return ','.join(file_list)
 

def strip_data(data):
    return data.strip()


def valid_email(email):
    exp = '(\w+[.|\w])*@(\w+[.])*\w+'
    return re.search(exp, email)


def get_email_list(data):
    xdata = data.split(email_seprator)
    emails = []
    for element in xdata:
        email = strip_data(element)
        if valid_email(email):
            emails.append(email)
    return emails


def get_next_ctrl_offset(index, rawMessage):
    offset = index
    for xindex in range(index, len(rawMessage)):
       if not (rawMessage[xindex].startswith(tuple(ctrl_list)) or rawMessage[index] == endOfHeader):
           offset = offset + 1
       else:
           break
    return offset


def custom_print(pair):
    key, value = pair
    print key, value
 
sc = SparkContext()
sc.setCheckpointDir("/user/pnda/checkpoint")
sqlContext = sql.SQLContext(sc)
row = Row("id")
row_1 = Row("src", "dst", "count")
rdd = sc.wholeTextFiles(path)
v_data = rdd.flatMap(get_vertex_list).distinct().map(row)
vertices = v_data.toDF()
#vertices.show()
e_data = rdd.flatMap(get_edge_list).filter(lambda list_: len(list_) > 0).collect()
e_data_ll = sc.parallelize(e_data)
edges = e_data_ll.toDF(["src", "dst", "count"])
#edges.show()
g = GraphFrame(vertices, edges)
g.vertices.show()
g.edges.show()
#g.inDegrees.show()
#g.outDegrees.show()
#result = g.connectedComponents()
#result.select("id", "component").orderBy("component").show()
g.edges.write.parquet("/user/pnda/result")

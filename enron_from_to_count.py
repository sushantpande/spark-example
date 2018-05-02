import re 
from pyspark import SparkContext, SparkConf

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
path = "/user/pnda/enron/enron/maildir/allen-p/inbox"
cr = '\n'

def parser(meta):
    name, rawMessage = meta
    rawMessage = rawMessage.split(cr)
    length = len(rawMessage)
    index = 0
    msg_Id = ''
    to_data = []
    from_data = []

    for line in rawMessage:
        line = line.strip()
        #print "===============================",line
        index = index + 1
        if line == '':
            body_data = ''.join(rawMessage[index:])
            break
        if line.startswith(_msgId):
            offset = get_next_ctrl_offset(index, rawMessage)
            msgId = line.split(':')[1] + ''.join(rawMessage[index: offset - 1])
        if line.startswith(_to):
            #print "**************************",line 
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
    return [(from_data , len(to_data))] 
    #return [(from_data , to_data)] 


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
rdd = sc.wholeTextFiles(path, 10).flatMap(parser).reduceByKey(lambda v1, v2 : v1 + v2)
#rdd = sc.textFile(path).flatMap(parser).reduceByKey(lambda v1, v2 : v1 + v2)
rdd.saveAsTextFile("/user/pnda/result")
#result =  rdd.collect()
#final_str = ""
#for item in result:
#    key = item[0]
#    value = item[1]
#    print key, value
    #for value in item[1]:
    #   print value


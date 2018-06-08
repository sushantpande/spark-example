import math
import copy
from graphframes import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark import sql


def split_query(target, col, op, key):
    max_query_args = 50
    size = len(target)
    batch_count = int(math.ceil(float(len(target))/max_query_args))
    query_list = []
    for index in range(0, batch_count):
        start = index * max_query_args
        end = start + max_query_args
        if end > size:
            end = size
        query_list.append(get_query(target[start:end], col, op, key))
    return query_list


def get_query(target, col, op, key):
        filter_q = ""
        for index in range(0, len(target)):
            if not filter_q:
                filter_q = filter_q + ' ' + col + '="' + target[index] + '"'
            else:
                filter_q = filter_q + ' ' + op + ' ' + col + '="' + target[index] + '"'
        return filter_q


def get_n_level_connection(root, n, all=False):
    correspondents = None
    for index in range(0, n):
        if not correspondents:
            query = 'src="' + root + '"'
            query_list = [query]
        else:
            query_list = split_query(correspondent_list, 'src', 'or', 'dst')
        if all:
            if correspondents:
                correspondents = DataFrame.unionAll(correspondents, reduce(DataFrame.unionAll, [e.filter(query) for query in query_list]))
            else:
                correspondents = reduce(DataFrame.unionAll, [e.filter(query) for query in query_list])
        else:
            correspondents = reduce(DataFrame.unionAll, [e.filter(query) for query in query_list])
        correspondent_list = correspondents.select("dst").rdd.map(lambda row : row.dst).collect() 
    return correspondents
 

def get_intersection(df1, df2):
    #inters_12_src_src =  DataFrame.intersect(df1.select("src"), df2.select("src"))
    #inters_12_src_dst =  DataFrame.intersect(df1.select("src"), df2.select("dst"))
    #inters_12_dst_src =  DataFrame.intersect(df1.select("dst"), df2.select("src"))
    inters_dst_dst =  DataFrame.intersect(df1.select("dst"), df2.select("dst"))

    inters_dst_dst_list = inters_dst_dst.select("dst").rdd.map(lambda row : row.dst).collect()

    query_list = []
    query_list += split_query(inters_dst_dst_list, 'src', 'or', 'dst')
    query_list += split_query(inters_dst_dst_list, 'dst', 'or', 'dst')

    inters_e = reduce(DataFrame.unionAll, [e.filter(query) for query in query_list])

    inters_v = inters_dst_dst.select(col("dst").alias("id"))
    return inters_v, inters_e


sc = SparkContext()
sc.setCheckpointDir("/user/pnda/checkpoint")
sqlContext = sql.SQLContext(sc)

#load vertices
v = sqlContext.read.parquet("/user/pnda/result/vertex")

#load edges
e = sqlContext.read.parquet("/user/pnda/result/edge")

eg = e.groupBy("src").count().sort("count", ascending=[0,1]).head(2)
user_1 = eg[0].src
user_2 = eg[1].src

df1 = get_n_level_connection(user_1, 3, True)
df2 = get_n_level_connection(user_2, 3, True)

inters_v, inters_e = get_intersection(df1, df2)

g = GraphFrame(inters_v, inters_e)

g.edges.write.parquet("/user/pnda/intersect_3_all/edge")
g.vertices.write.parquet("/user/pnda/intersect_3_all/vertex")

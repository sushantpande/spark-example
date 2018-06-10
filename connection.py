from pyspark.sql import Row
from pyspark.sql.functions import lit
from graphframes import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql


row = ["df_src", "df_dst", "df_mcount"]
def get_n_level_connection(root, n, all=False):
    self_ref = 'dst !="' +  root + '"'
    e = e_.filter(self_ref)
    df = None
    final_df = None
    for index in range(0, n):
       if not df:
           query = 'src="' + root + '"'
           df = e.filter(query)
           df = df.toDF(*row)
           df = df.withColumn("distance", lit(index + 1)) 
           final_df = df
       else:
           df = e.join(df, df.df_dst == e.src).select("src", "dst", "mcount").distinct()
           df = df.toDF(*row)
           df = df.withColumn("distance", lit(index + 1))
           final_df = df.unionAll(final_df).distinct()  
    row_ =  ["src", "dst", "mcount", "distance"]
    return final_df.toDF(*row_)



sc = SparkContext()
sc.setCheckpointDir("/user/pnda/checkpoint")
sqlContext = sql.SQLContext(sc)

#load vertices
v = sqlContext.read.parquet("/user/pnda/result/vertex")

#load edges
e_ = sqlContext.read.parquet("/user/pnda/result/edge")

eg = e_.groupBy("src").count().sort("count", ascending=[0,1]).head(3)
user_1 = eg[0].src
user_2 = eg[1].src

df = get_n_level_connection(user_1, 3)

v1 = df.select("src") 
v2 = df.select("dst")
row = ["id"]
vertex = v1.unionAll(v2).distinct().toDF(*row) 

g = GraphFrame(vertex, df)
g.edges.write.parquet("/user/pnda/level3/edge")
g.vertices.write.parquet("/user/pnda/level3/vertex")

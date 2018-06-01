from graphframes import *
sc.setCheckpointDir("/user/pnda/checkpoint")
e = sqlContext.read.parquet("/user/pnda/result/edge")
v = sqlContext.read.parquet("/user/pnda/result/vertex")
e.filter("""src='liz.taylor@enron.com'""").show()
e.filter("""src='kalmeida@caiso.com'""").filter("""dst='chris.stokley@enron.com'""").show()
g = GraphFrame(v,e)
g.bfs("id='neil.davies@enron.com'", "id = 'caroline.emmert@enron.com'").show()

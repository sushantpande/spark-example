from graphframes import *

#load vertices
v = sqlContext.read.parquet("/user/pnda/result/vertex")

#load edges
e = sqlContext.read.parquet("/user/pnda/result/edge")

#run sample queries
e.filter("""src='liz.taylor@enron.com'""").show()
e.filter("""src='kalmeida@caiso.com'""").filter("""dst='chris.stokley@enron.com'""").show()

#create graph
g = GraphFrame(v,e)

#get all paths between A and B
g.bfs("id='neil.davies@enron.com'", "id = 'caroline.emmert@enron.com'").show()

#connected components
#sc.setCheckpointDir("/user/pnda/checkpoint")
#result = g.connectedComponents()
#result.select("id", "component").orderBy("component").show()
#e.filter("""src='ken.lay-@enron.com'""").show()
#e.filter("""src='all.worldwide@enron.com'""").show()

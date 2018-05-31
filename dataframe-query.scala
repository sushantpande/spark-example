val parqfile = sqlContext.read.parquet("/user/pnda/result/edge")
parqfile.show()
parqfile.filter("""src='louise.kitchen@enron.com'""").select("count").count()
parqfile.filter("""src='louise.kitchen@enron.com'""").filter("""dst='louise.kitchen@enron.com'""").select("count").count()

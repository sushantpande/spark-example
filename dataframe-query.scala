val parqfile = sqlContext.read.parquet("/user/pnda/result/edge")
parqfile.show()
parqfile.filter("""src='liz.taylor@enron.com'""").show()
parqfile.filter("""src='kalmeida@caiso.com'""").filter("""dst='chris.stokley@enron.com'""").show()

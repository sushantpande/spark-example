#Execute on local 
spark-submit --master local spark_message_parser.py
#Execute on yarn cluster
spark-submit --master yarn-cluster spark_message_parser.py

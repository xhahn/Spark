/home/spark/spark-1.2.0-bin-hadoop2.4/bin/spark-submit \
--class com.xhahn.mySparkNaiveBayes.NBayes \
--name "NBayes" \
--master spark://192.168.1.2:7077 \
/home/spark/xh/ideaProject/NBayes/out/artifacts/NBayes_jar/NBayes.jar \
hdfs://master:9000/xh/my/data/NaiveBayes/input/trainData.txt \
hdfs://master:9000/xh/my/data/NaiveBayes/input/testData.txt

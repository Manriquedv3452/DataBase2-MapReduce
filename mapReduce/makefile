#MAP REDUCE 1
createMapreduce1:
	javac -classpath $(shell hadoop classpath) -d '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' '/home/manriquedv/Escritorio/mapReduce/UserHashtagCount.java' 
	jar -cvf mapReduce1.jar -C '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' . 
	

executeMapreduce1:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar '/home/manriquedv/Escritorio/mapReduce/mapReduce1.jar' UserHashtagCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/*

#MAP REDUCE 2
createMapreduce2: 
	javac -classpath $(shell hadoop classpath) -d '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' '/home/manriquedv/Escritorio/mapReduce/TweetPerHashtagCount.java' 
	jar -cvf mapReduce2.jar -C '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' . 

executeMapreduce2:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar '/home/manriquedv/Escritorio/mapReduce/mapReduce2.jar' TweetPerHashtagCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/*


#MAP REDUCE 3
createMapreduce3:
	javac -classpath $(shell hadoop classpath) -d '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' '/home/manriquedv/Escritorio/mapReduce/TweetsWordCount.java' 
	jar -cvf mapReduce3.jar -C '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' . 

executeMapreduce3:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar '/home/manriquedv/Escritorio/mapReduce/mapReduce3.jar' TweetsWordCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/*


#MAPREDUCE 4
createMapreduce4:
	javac -classpath $(shell hadoop classpath) -d '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' '/home/manriquedv/Escritorio/mapReduce/TweetOtherHashtagCount.java' 
	jar -cvf mapReduce4.jar -C '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' . 

executeMapreduce4:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar '/home/manriquedv/Escritorio/mapReduce/mapReduce4.jar' TweetOtherHashtagCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/*

#MAP REDUCE 5
createMapreduce5:
	javac -classpath $(shell hadoop classpath) -d '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' '/home/manriquedv/Escritorio/mapReduce/TweetsPerHour.java' 
	jar -cvf mapReduce5.jar -C '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' . 

executeMapreduce5:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar '/home/manriquedv/Escritorio/mapReduce/mapReduce5.jar' TweetsPerHour /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/*

#MAP REDUCE 6
createMapreduce6:
	javac -classpath $(shell hadoop classpath) -d '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' '/home/manriquedv/Escritorio/mapReduce/TweetMainHashtagCount.java' 
	jar -cvf mapReduce6.jar -C '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' . 

executeMapreduce6:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar '/home/manriquedv/Escritorio/mapReduce/mapReduce6.jar' TweetMainHashtagCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/*

data:
	hadoop fs -rm /mapReduce/Input/tweets.txt
	hadoop fs -put '/home/manriquedv/Escritorio/mapReduce/input_data/tweets.txt' /mapReduce/Input

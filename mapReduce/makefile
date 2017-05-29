create:
	javac -classpath $(shell hadoop classpath) -d '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' '/home/manriquedv/Escritorio/mapReduce/UserHashtagCount.java' 
	jar -cvf mapReduce.jar -C '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' . 
	

execute:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar '/home/manriquedv/Escritorio/mapReduce/mapReduce.jar' UserHashtagCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/*

create1: 
	javac -classpath $(shell hadoop classpath) -d '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' '/home/manriquedv/Escritorio/mapReduce/TweetHashtagCount.java' 
	jar -cvf mapReduceTweets.jar -C '/home/manriquedv/Escritorio/mapReduce/mapReduce_classes' . 

execute1:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar '/home/manriquedv/Escritorio/mapReduce/mapReduceTweets.jar' TweetHashtagCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/*

data:
	hadoop fs -rm /mapReduce/Input/tweets.txt
	hadoop fs -put '/home/manriquedv/Escritorio/mapReduce/input_data/tweets.txt' /mapReduce/Input

#MAP REDUCE 1 y 2
createMapreduce1:
	javac -classpath $(shell hadoop classpath) -d 'mapReduce_classes/' 'UserTweetHashtagCount.java' 
	jar -cvf mapReduce1-2.jar -C mapReduce_classes/ . 
	

executeMapreduce1:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar 'mapReduce1-2.jar' UserTweetHashtagCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/* > 'resultMapReduce1.txt'


#MAP REDUCE 3
createMapreduce3:
	javac -classpath $(shell hadoop classpath) -d 'mapReduce_classes/' 'TweetsWordCount.java' 
	jar -cvf mapReduce3.jar -C mapReduce_classes/ . 

executeMapreduce3:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar 'mapReduce3.jar' TweetsWordCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/* > 'resultMapReduce3.txt'


#MAPREDUCE 4
createMapreduce4:
	javac -classpath $(shell hadoop classpath) -d 'mapReduce_classes/' 'TweetOtherHashtagCount.java' 
	jar -cvf mapReduce4.jar -C mapReduce_classes/ . 

executeMapreduce4:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar 'mapReduce4.jar' TweetOtherHashtagCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/* > 'resultMapReduce4.txt'

#MAP REDUCE 5
createMapreduce5:
	javac -classpath $(shell hadoop classpath) -d 'mapReduce_classes/' 'TweetsPerHour.java' 
	jar -cvf mapReduce5.jar -C mapReduce_classes/ . 

executeMapreduce5:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar 'mapReduce5.jar' TweetsPerHour /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/* > 'resultMapReduce5.txt'

#MAP REDUCE 6
createMapreduce6:
	javac -classpath $(shell hadoop classpath) -d 'mapReduce_classes/' 'TweetMainHashtagCount.java' 
	jar -cvf mapReduce6.jar -C mapReduce_classes/ . 

executeMapreduce6:
	hadoop fs -rm -r /mapReduce/Output
	hadoop jar 'mapReduce6.jar' TweetMainHashtagCount /mapReduce/Input /mapReduce/Output
	hadoop dfs -cat /mapReduce/Output/* > 'resultMapReduce6.txt'

data:
	hadoop fs -put '/input_data/tweets.txt' /mapReduce/Input

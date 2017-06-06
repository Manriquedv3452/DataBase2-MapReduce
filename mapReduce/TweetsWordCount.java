import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.lang.String;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TweetsWordCount {

  public static class TweetMapper extends Mapper<Object, Text, Text, Text>{

    private Text tKey = new Text();
    private String[] topics = {"2030now", "women", "costarica", "puravida", "makeamericagreatagain", "trumprussia", "recyclereuse", "traficocr"};
    private String[] prepositions = {"and", "the","sobre","about","encima","above","través","across","después","after","contra","against","entre","among","alrededor","around","como","as","en","at","antes","before","detrás","behind","debajo","below","bajo","beneath","al","lado","beside","entre","between","allá","beyond","pero","but","por","by","abajo","down","durante","during","salvo","except","para","for","de","from","en","in","dentro","inside","en","into","cerca","near","próximo","next","de","of","en","on","opuesto","opposite","fuera","out","fuera","outside","encima","over","por","per","más","plus","alrededor","round","desde","since","que","than","través","through","hasta","till","a","to","hacia","toward","bajo","under","hasta","until","arriba","up","vía","via","con","with","dentro","within","sin","without"};
    private String[] usedTopics = new String[200];
    private String word = "";

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //StringTokenizer tweets = new StringTokenizer(value.toString());;
      String tweet = value.toString();
      String hashtag = tweet.split("%")[2];
      String tweetContent = tweet.split("%")[3];
      char[] tweetChars = tweetContent.toCharArray();

      //Take all the hashtags off
      for (int i = 0; i < tweetChars.length; i++)
      {
        if (tweetChars[i] == '#')
        {
          while (i < tweetChars.length && tweetChars[i] != ' ')
          {
            tweetChars[i] = 0;
            i++;
          }
          if (i >= tweetChars.length)
          {
                break;
          }
        }
      }
      //Set the tweet content and clear all special characters
      tweetContent = String.valueOf(tweetChars);
      tweetContent = tweetContent.replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+", " ");

      //If hashtag is not a blank
      if (hashtag.length() > 1){

        String[] hashtags = hashtag.split("#");
        usedTopics = new String[200];

        //Set the hashtag as key and all the values of this one
        for (int i = 0;  i < hashtags.length; i++)
        {
          if (Arrays.asList(topics).contains(hashtags[i].toLowerCase()) && !Arrays.asList(usedTopics).contains(hashtags[i].toLowerCase()))
          {
            usedTopics[i] = hashtags[i].toLowerCase();
            StringTokenizer itr = new StringTokenizer(tweetContent);

            //Getting the word from the sentence
            while (itr.hasMoreTokens()) {

              word = itr.nextToken();
              word = word.replaceAll("\b", "");
              if (!Arrays.asList(prepositions).contains(word.toLowerCase()) && word.length() > 2)
              {
                tKey.set(new Text(hashtags[i].toLowerCase()));
                context.write(tKey, new Text(word.toLowerCase()));
              }
            }
          }
        }
      }
    }
  }

  public static class TweetReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
      HashMap<String, Integer> words = new HashMap<String, Integer>();

      for (Text val : values) {

          String word = val.toString();

          if(words.get(word) == null)
          {
            words.put(word, 1);
          }
          else{
            int value = words.get(word) + 1; 
            words.put(word,value);
          }
      }
      Map sortedMap = sortByValues(words);
      String top10 = "";
      int counter = 0;
      for (Object word : sortedMap.keySet()) {
          if (counter ++ == 10) {
              break;
          }
          if (word.toString().length() > 0){
            top10 += word.toString() + "=" + sortedMap.get(word) + "#";
          }
      }

      context.write(key, new Text(top10));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.set("mapred.textoutputformat.separator", ",");
    Job job = Job.getInstance(conf, "hashtagWordCount");
    job.setJarByClass(TweetsWordCount.class);
    job.setMapperClass(TweetMapper.class);
    job.setCombinerClass(TweetReducer.class);
    job.setReducerClass(TweetReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


  //Taken from http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
  public static <K extends Comparable,V extends Comparable> Map<K,V> sortByValues(Map<K,V> map){
    List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
 
    Collections.sort(entries, new Comparator<Map.Entry<K,V>>() {

        @Override
        public int compare(Entry<K, V> object1, Entry<K, V> object2) {
            return object2.getValue().compareTo(object1.getValue());
        }
    });

    //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K,V> sortedMap = new LinkedHashMap<K,V>();
     
        for(Map.Entry<K,V> entry: entries){
            sortedMap.put(entry.getKey(), entry.getValue());
        }
     
        return sortedMap;
  }



}

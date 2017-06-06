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

public class TweetMainHashtagCount {

  public static class TweetMapper extends Mapper<Object, Text, Text, Text>{

    private Text tKey = new Text();
    private String[] topics = {"2030now", "women", "costarica", "puravida", "makeamericagreatagain", "trumprussia", "recyclereuse", "traficocr"};
    private String[] usedTopics = new String[200];
    private String[] usedHashtags = new String[200];

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //StringTokenizer tweets = new StringTokenizer(value.toString());;
      String tweet = value.toString();
      if (tweet.split("%").length >= 4){
        String hashtag = tweet.split("%")[2];
        String tweetContent = tweet.split("%")[3];
        char[] tweetChars = tweetContent.toCharArray();


        //If hashtag is not a blank
        if (hashtag.length() > 1){

          String[] hashtags = hashtag.split("#");
          usedTopics = new String[200];

          //Set the hashtag as key and all the values of this one
          for (int i = 0;  i < hashtags.length; i++)
          {
            usedHashtags = new String[200];
            String keyHashtag = hashtags[i].toLowerCase();
            if (Arrays.asList(topics).contains(keyHashtag) && !Arrays.asList(usedTopics).contains(keyHashtag) && keyHashtag != "")
            {
              usedTopics[i] = keyHashtag;
              tKey.set(new Text(keyHashtag));

              //Get other hashtags
              for(int j = 0; j < hashtags.length; j++)
              {
                String otherHashtag = hashtags[j].toLowerCase();
                if (!keyHashtag.equals(otherHashtag) && Arrays.asList(topics).contains(otherHashtag) && !Arrays.asList(usedHashtags).contains(otherHashtag) && otherHashtag != "")
                {
                  usedHashtags[j] = otherHashtag;
                  context.write(tKey, new Text(otherHashtag));

                }
              }
            }
          }
        }
      }//END VALIDATION
    }
  }

  public static class TweetReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      HashMap<String, Integer> hashtags = new HashMap<String, Integer>();

      for (Text val : values) {

          String hashtag = val.toString();

          if(hashtags.get(hashtag) == null)
          {
            hashtags.put(hashtag, 1);
          }
          else{
            int value = hashtags.get(hashtag) + 1; 
            hashtags.put(hashtag,value);
          }
      }
      Map sortedMap = sortByValues(hashtags);
      String top10 = "";
      int counter = 0;
      for (Object hashtag : sortedMap.keySet()) {
          if (counter == 10) {
              break;
          }
          if (hashtag.toString().length() > 0){
            top10 += hashtag.toString() + "=" + sortedMap.get(hashtag) + "#";
          }
          counter++;
      }

      context.write(key, new Text(top10));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", "%");
    Job job = Job.getInstance(conf, "otherHashtagCount");
    job.setJarByClass(TweetMainHashtagCount.class);
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

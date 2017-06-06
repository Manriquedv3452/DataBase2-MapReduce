import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TweetsPerHour {

  public static class TweetMapper extends Mapper<Object, Text, Text, Text>{

    private Text tKey = new Text();
    private String[] topics= {"2030now", "women", "costarica", "puravida", "makeamericagreatagain", "trumprussia", "recyclereuse", "traficocr"};
    private String[] usedTopics = new String[200];


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //StringTokenizer tweets = new StringTokenizer(value.toString());
      String tweetHour = "";
      String tweetDate = "";
      String tweetContent = "";
      String tweet = value.toString();
      
      if (tweet.split("%").length >= 4){
        String hashtag = tweet.split("%")[2];
        if (hashtag.length() > 1){
          String[] hashtags = hashtag.split("#");
          usedTopics = new String[200];
          tweetHour = tweet.split("%")[1].split(" ")[1].split(":")[0];
          tweetDate = tweet.split("%")[1].split(" ")[0];
          for (int i = 0;  i < hashtags.length; i++)
          {
            //System.out.println(hashtags[i]);
            if (Arrays.asList(topics).contains(hashtags[i].toLowerCase()) && !Arrays.asList(usedTopics).contains(hashtags[i].toLowerCase()))
            {
              usedTopics[i] = hashtags[i].toLowerCase();
              tKey.set(new Text(hashtags[i].toLowerCase()));
              context.write(tKey, new Text(tweetDate + "-" + tweetHour));
            }
          }
        }
      }//END VALIDATION
    }
  }

  public static class TweetReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      HashMap<String, Integer> dateHours = new HashMap<String, Integer>();

      for (Text val : values) {
          
          String dateHour = val.toString();
          
          if(dateHours.get(dateHour) == null)
          {
            dateHours.put(dateHour, 1);
          }
          else{
            int value = dateHours.get(dateHour) + 1; 
            dateHours.put(dateHour,value);
          }
      }
      String dateHourValues = "";
      for (Object dateHour : dateHours.keySet()) {
          if (dateHour.toString().length() > 0){
            dateHourValues += dateHour.toString() + "=" + dateHours.get(dateHour) + "#";
          }
      }

      context.write(key, new Text(dateHourValues));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", "%");
    Job job = Job.getInstance(conf, "tweetsPerHour");
    job.setJarByClass(TweetsPerHour.class);
    job.setMapperClass(TweetMapper.class);
    job.setCombinerClass(TweetReducer.class);
    job.setReducerClass(TweetReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

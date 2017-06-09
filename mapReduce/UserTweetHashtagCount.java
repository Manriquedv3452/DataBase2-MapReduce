import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserTweetHashtagCount {

  public static class TweetMapper extends Mapper<Object, Text, Text, Text>{

    //private final static IntWritable tValue = new IntWritable(1);
    private Text tKey = new Text();
    private String[] topics= {"2030now", "women", "costarica", "puravida", "makeamericagreatagain", "trumprussia", "recyclereuse", "traficocr"};
    private String[] usedTopics = new String[200];

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //StringTokenizer tweets = new StringTokenizer(value.toString());
      String tweetUser = "";
      String tweet = value.toString();

      if (tweet.split("%").length >= 4){
        String hashtag = tweet.split("%")[2];

        if (hashtag.length() > 1){
          usedTopics = new String[200];
          String[] hashtags = hashtag.split("#");
          tweetUser = tweet.split("%")[0];

          for (int i = 0;  i < hashtags.length; i++)
          {
            //System.out.println(hashtags[i]);
            if (Arrays.asList(topics).contains(hashtags[i].toLowerCase()) && !Arrays.asList(usedTopics).contains(hashtags[i].toLowerCase()))
            {
              usedTopics[i] = hashtags[i].toLowerCase();
              tKey.set(new Text(hashtags[i].toLowerCase() + "%" + tweetUser));
              context.write(tKey, new Text("1,1"));
            }
          }
        }
      }//END VALIDATION
    }
  }

  public static class TweetReducer extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      int tweetsCount = 0;
      for (Text val : values) {
        if (val.toString().split(",")[1].equals("1"))
        {
          count += Integer.parseInt(val.toString().split(",")[1]);
        }
        else{
          count += 1;
        }
        tweetsCount += Integer.parseInt(val.toString().split(",")[0]);

      }

      context.write(new Text(key.toString().split("%")[0]), new Text(tweetsCount + "," + count));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", "=");
    Job job = Job.getInstance(conf, "userAndTweetsCount");
    job.setJarByClass(UserTweetHashtagCount.class);
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

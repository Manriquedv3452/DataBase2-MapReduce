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

public class UserHashtagCount {

  public static class TweetMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable tValue = new IntWritable(1);
    private Text tKey = new Text();
    private String[] topics= {"2030now", "women", "costarica", "puravida", "makeamericagreatagain", "trumprussia", "recyclereuse", "traficocr"};

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //StringTokenizer tweets = new StringTokenizer(value.toString());
      String tweetUser = "";
      String tweetContent = "";
      String tweet = value.toString();
      if (tweet.split("%").length >= 4){
        String hashtag = tweet.split("%")[2];
        if (hashtag.length() > 1){
          String[] hashtags = hashtag.split("#");
          tweetUser = tweet.split("%")[0];
          for (int i = 0;  i < hashtags.length; i++)
          {
            //System.out.println(hashtags[i]);
            if (Arrays.asList(topics).contains(hashtags[i].toLowerCase()))
            {

              tKey.set(new Text(hashtags[i].toLowerCase() + "%" + tweetUser));
              context.write(tKey, tValue);
            }
          }
        }
      }//END VALIDATION
    }
  }

  public static class TweetReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable val : values) {
        if (val.get() == 1)
        {
          count += val.get();
        }
        else{
          count += 1;
        }

      }
      result.set(count);

      context.write(new Text(key.toString().split("%")[0]), result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", "=");
    Job job = Job.getInstance(conf, "userCount");
    job.setJarByClass(UserHashtagCount.class);
    job.setMapperClass(TweetMapper.class);
    job.setCombinerClass(TweetReducer.class);
    job.setReducerClass(TweetReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

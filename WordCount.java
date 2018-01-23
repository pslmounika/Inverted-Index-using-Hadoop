import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Reducer;


public class WordCount {

  public static class WordCountMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text docId = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      docId.set(itr.nextToken());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, docId);
      }
    }
  }

  public static class WordCountReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      HashMap<String,Integer> Inputmap = new HashMap<String,Integer>();      
      for (Text val : values) {
        if(Inputmap.containsKey(val.toString()))
        {
                Inputmap.put(val.toString(),Inputmap.get(val.toString())+1);
        }
        else
        {
                Inputmap.put(val.toString(),1);
        }

      }

      context.write(key, new Text(Inputmap.toString().replace('=',':').replace("," , "    ").replace('{',' ').replace('}',' ')));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration config= new Configuration();
    Job jobConfig = Job.getInstance(config, "word count");
    jobConfig.setMapperClass(WordCountrMapper.class);
    jobConfig.setReducerClass(WordCountReducer.class);
    jobConfig.setJarByClass(WordCount.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
   jobConfig.setOutputKeyClass(Text.class);
    jobConfig.setOutputValueClass(Text.class);
    
    
    System.exit(jobConfig.waitForCompletion(true) ? 0 : 1);
  }
}

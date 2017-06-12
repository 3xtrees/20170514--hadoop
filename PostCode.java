package org._3xtrees.postcode;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PostCode {

  public static List<String> myTokenizer(String str)
  {
    String []stra = str.split("\"");
    int i = 0;
    String []temp;
    List<String> result = new ArrayList<String>();
    for(String s : stra)
    {
      if(i % 2 == 0)
      {
        temp = s.split(",");
        if(temp.length > 0)
        {
          for(String ts : temp)
          result.add(ts);
        }
      }else
      {
        result.add(s);
      }
      i++;
    }
    return result;
  }

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
  {
    private IntWritable price = new IntWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      //StringTokenizer itr = new StringTokenizer(value.toString());
      List<String> record = myTokenizer(value.toString());
      if(record.get(4).equals("D")&&record.get(14).equals("A")&&record.get(15).equals("A"))
      {
        int temp_price = Integer.parseInt(record.get(1));
        if(temp_price>4000000&&temp_price<5000000)
        {
          price.set(temp_price);
          word.set("\""+record.get(3)+"\"");
          context.write(word, price);
        }
      }
    }
  }

  public static class IntMaxReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {
      int max = 0;
      for (IntWritable val : values) 
      {
        int temp = val.get();
        if(max<temp)
        {
          max = temp;
        }
      }
      result.set(max);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "PostCode");
    job.setJarByClass(PostCode.class);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntMaxReducer.class);
    job.setReducerClass(IntMaxReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

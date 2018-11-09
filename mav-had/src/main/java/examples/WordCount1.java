package examples;
import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount1 
{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
  {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
    	//System.out.println("HEY map: "+value); 
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) 
      {
        word.set(itr.nextToken());
        context.write(word, one);
      //  System.out.println("Write map "+ word); 
      }
    }//map
  }//mapper

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
    {
      int sum = 0;
     // System.out.println("HEY reducer: "+String.valueOf(values)); 
      for (IntWritable val : values) 
      {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
     // System.out.println("Write reducer "+ key + "and sum is: "+String.valueOf(sum)); 
    }//reduce
  }//reducer

  public static void main(String[] args) throws Exception 
  {
	
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(Integer.valueOf(args[2]));
    
    String input="/home/razold-master/workspace_eclipse/mav-had/MyInput/book.txt";
	Random rand = new Random();
	int  n = rand.nextInt(1000) + 1;
	String output="/home/razold-master/workspace_eclipse/mav-had/MyOutput/book/"+String.valueOf(n);
	
	//FileInputFormat.addInputPath(job, new Path(input));    
    FileInputFormat.addInputPath(job, new Path(args[0]));
	//FileOutputFormat.setOutputPath(job, new Path(output)); 
    FileOutputFormat.setOutputPath(job, new Path(args[1]));  
    System.out.println("WordCountEx");
	System.out.println("Args:\n Input- "+input+" \n Output- "+output);  
	
	long start = new Date().getTime();
    job.waitForCompletion(true);
    long end = new Date().getTime();
    System.out.println("Job took "+(end-start) + " milliseconds");
  }
}//wordcount1

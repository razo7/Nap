package examples;
//cc MaxTemperature Application to find the maximum temperature in the weather dataset
//vv MaxTemperature
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Random;

public class MaxTemperature 
{
		
	//^^ MaxTemperature
	//cc MaxTemperatureMapper Mapper for maximum temperature example
	//vv MaxTemperatureMapper

	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
	{

		private static final int MISSING = 9999;
		//public void init(){}
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
		 
			 //System.out.println("HEY map"); 
			 String line = value.toString();
			 String year = line.substring(15, 19);
			 int airTemperature;
			 if (line.charAt(87) == '+') 
			 { // parseInt doesn't like leading plus signs
			   airTemperature = Integer.parseInt(line.substring(88, 92));
			 } 
			 else 
			 {
			   airTemperature = Integer.parseInt(line.substring(87, 92));
			 }
			 String quality = line.substring(92, 93);
			 if (airTemperature != MISSING && quality.matches("[01459]")) 
			 {
			   context.write(new Text(year), new IntWritable(airTemperature));
			  // System.out.println("Write MAP");
			 }
		}//map
		
	}//mapper
	

	//^^ MaxTemperatureMapper
	//cc MaxTemperatureReducer Reducer for maximum temperature example
	//vv MaxTemperatureReducer

	public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException 
		{
		
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) 
			{
			maxValue = Math.max(maxValue, value.get());
			}
		context.write(key, new IntWritable(maxValue));
		//System.out.println("Write");
		}//reduce
	}//reducer
	//^^ MaxTemperatureReducer
	
	public static void main(String[] args) throws Exception 
	{
	 if (args.length != 2) 
	 {
	   System.err.println("Usage: MaxTemperature <input path> <output path>");
	   System.exit(-1);
	 }
	 
	 Configuration conf = new Configuration();
	 Job job = Job.getInstance(conf, "Max temperature");
	 String input="/home/razold-master/workspace_eclipse/MyInput/2001.gz";
	 Random rand = new Random();
	 int  n = rand.nextInt(1000) + 1;
	 String output="/home/razold-master/workspace_eclipse/MyOutput/testMaxTemp/"+String.valueOf(n);
	 job.setJarByClass(MaxTemperature.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));

	 //FileInputFormat.addInputPath(job, new Path(input));
	 //FileOutputFormat.setOutputPath(job, new Path(output));
/*
 * 
Path HdpPath = new Path(args[0]);
Path ClouderaPath = new Path(args[1]);
Path outputPath = new Path(args[2]);
MultipleInputs.addInputPath(job, ClouderaPath, TextInputFormat.class, JoinclouderaMapper.class);
MultipleInputs.addInputPath(job, HdpPath, TextInputFormat.class, HdpMapper.class);
FileOutputFormat.setOutputPath(job, outputPath);	 
 */
	 
	 job.setMapperClass(MaxTemperatureMapper.class);
	 job.setCombinerClass(MaxTemperatureReducer.class);
	 job.setReducerClass(MaxTemperatureReducer.class);
	
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(IntWritable.class);
	 System.out.println("MaxTemperatureEx");
	 System.out.println("Args:\n Input- "+input+" \n Output- "+output);
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}//main
	
}//MaxTemperature


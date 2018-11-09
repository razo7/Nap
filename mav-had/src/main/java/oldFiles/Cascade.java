package oldFiles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
/*
 * **********************************************
 *                                              *
 * Comp 6521 - Advance Database Applications    *
 * Project # 1                                  *
 * Implement Natural Join using Hadoop          *
 *                                              *
 * Developed By:                                *
 * Muhammad Umer (40015021)                     *
 * Hamzah Hamdi                                 *
 *                                              *
 * **********************************************
 */
public class Cascade 
{
	
	public static class RMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		 
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
	         String line = value.toString();
	         String[] splitInput = line.split("\\t");
	
	         Text a = new Text(splitInput[0]);
	         Text b = new Text(splitInput[1]);
	
	         Text opString = new Text("R"+a.toString());
	
	         output.collect(b, opString);
	     }//map
		
	}//RMapper
	
	public static class SMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			String line = value.toString();
			String[] splitInput = line.split("\\t");
			
			Text b = new Text(splitInput[0]);
			Text c = new Text(splitInput[1]);
			
			Text opString = new Text("S"+c.toString());
	
			output.collect(b, opString);
	
		}//map
	
	}//SMapper
	
	public static class TMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		 
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
	         String line = value.toString();
	         String[] splitInput = line.split("\\t");
	
	         Text c = new Text(splitInput[0]);
	         Text a = new Text(splitInput[1]);
	
	         Text opString = new Text("T"+a.toString());
	
	         output.collect(c, opString);
	     }//map
		
	}//TMapper
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
	{
	
		ArrayList < Text > RelS;
		ArrayList < Text > RelR; 
	
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
	
			RelS = new ArrayList < Text >() ;
			RelR = new ArrayList < Text >() ;
			
			while (values.hasNext()) 
			{
				String relationValue = values.next().toString();
	
				if (relationValue.indexOf('R') >= 0)
				{
					String finalVal = relationValue.substring(1, relationValue.length());
					RelR.add(new Text(finalVal));
				} else 
				{
					String finalVal = relationValue.substring(1, relationValue.length());
					RelS.add(new Text(finalVal));
				}
			}
	
			for( Text r : RelR ) 
				{
				for (Text s : RelS) 
					output.collect(new Text(), new Text(r + "," + key.toString() + "," + s));
				}
		}
	
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public static class R1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		 
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	         String line = value.toString().trim();
	         String[] splitInput = line.split(",");
	
	         Text a = new Text(splitInput[0]);
	         Text b = new Text(splitInput[1]);
	         Text c = new Text(splitInput[2]);
	
	         Text opString = new Text("R1"+a.toString()+","+b.toString());
	
	         output.collect(c, opString);
	     }//map
		
	}//R1Mapper
	
	public static class J2Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
	{
	
		ArrayList < Text > RelR1;
		ArrayList < Text > RelT; 
	
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			RelR1 = new ArrayList < Text >() ;
			RelT = new ArrayList < Text >() ;
			
			while (values.hasNext()) 
			{
				String relationValue = values.next().toString();
	
				if (relationValue.indexOf("R1") >= 0)
				{
					String finalVal = relationValue.substring(2, relationValue.length());
					RelR1.add(new Text(finalVal));
				} else 
				{
					String finalVal = relationValue.substring(1, relationValue.length());
					RelT.add(new Text(finalVal));
				}
			}
	
			for( Text r : RelR1 ) 
			{
				for (Text s : RelT) 
					output.collect(new Text(), new Text(r + "," + key.toString() + "," + s));
			}
		}
	
	}//J2Reducer
	
	
	
	    public static void main(String[] args) throws Exception 
	    {
	    	long start = new Date().getTime();
	    	 
	        JobConf conf = new JobConf(MultipleInputs.class);
	        conf.setJobName("Comp 6521 Project 2 - Natural Join");
	
	        conf.setOutputFormat(SequenceFileOutputFormat.class);
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        
	        conf.setReducerClass(Reduce.class);
	
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	
	        //conf.setNumReduceTasks(32);
		     
	        MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, RMapper.class);
	        MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, SMapper.class);
	
	
	        FileOutputFormat.setOutputPath(conf, new Path("tempOutputCascade/"));
	        JobClient.runJob(conf);
	        
	        //===================================================================
	        JobConf conf2 = new JobConf(MultipleInputs.class);
	        conf2.setJobName("Comp 6521 Project 2 - Natural Join");
	
	        conf2.setOutputKeyClass(Text.class);
	        conf2.setOutputValueClass(Text.class);
	        
	        conf2.setReducerClass(J2Reducer.class);
	
	        conf2.setInputFormat(TextInputFormat.class);
	        conf2.setOutputFormat(TextOutputFormat.class);
	        
		     
	        MultipleInputs.addInputPath(conf2, new Path(args[2]), TextInputFormat.class, TMapper.class);
	        MultipleInputs.addInputPath(conf2, new Path("tempOutputCascade/"), TextInputFormat.class, R1Mapper.class);
	
	
	        FileOutputFormat.setOutputPath(conf2, new Path("CascadeFinalOutput/"));
	       
	        
	        
	        JobClient.runJob(conf2);
	        
	        
	        long end = new Date().getTime();
	        System.out.println("Job took "+(end-start) + "milliseconds");
	
	    }//main

}//Cascade

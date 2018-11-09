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

public class OneWay2 
{
	private static final int reducerNum = 12;
	private static final int tablesNum = 3;
	private static final int CNum = 2;
	private static final int ANum = 2;
	private static final int BNum = 3;
	public static String reducerIndex;

	public static class RMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			 System.out.println("HEY map1: "+value);  
			 String line = value.toString();
	         String[] splitInput = line.split("\\t");

	         Text a = new Text(splitInput[0]);
	         Text b = new Text(splitInput[1]);

	         Text opString = new Text("R"+a+","+b);
	         String a1= String.valueOf(1+ (splitInput[0].hashCode()& Integer.MAX_VALUE % CNum));
	         String b1= String.valueOf(1+ (splitInput[1].hashCode()& Integer.MAX_VALUE % CNum));
	         for(int i=1; i<= CNum; i++)
	         	{
	        	 	String c1= String.valueOf(i);
	        		reducerIndex= a1+b1+c1;
	        		System.out.println(reducerIndex);  
	        	 	output.collect(new Text(reducerIndex), opString); 
				}
					
	     //  output.collect(new Text("1"), opString); //was in comment
	     }//map
		
	}//RMapper Class
	
	public static class SMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			System.out.println("HEY map2: "+value); 
			String line = value.toString();
			String[] splitInput = line.split("\\t");
			
			Text b = new Text(splitInput[0]);
			Text c = new Text(splitInput[1]);
			
			Text opString = new Text("S"+b+","+c);
			String b1= String.valueOf(1+(splitInput[0].hashCode()& Integer.MAX_VALUE % ANum));
			String c1= String.valueOf(1+(splitInput[1].hashCode()& Integer.MAX_VALUE % ANum));
	        for(int i=1; i<= ANum; i++)
	         	{
	        		String a1= String.valueOf(i);
	        		reducerIndex= a1+b1+c1;
	        		System.out.println(reducerIndex);
	        	 	output.collect(new Text(reducerIndex), opString); 
				}
	         //output.collect(new Text("1"), opString);

		}//map

	}//SMapper Class
	
	
	public static class TMapper extends MapReduceBase implements Mapper<LongWritable, Text,Text, Text> 
	{
		 
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			 System.out.println("HEY map3: "+value); 
			 String line = value.toString();
	         String[] splitInput = line.split("\\t");

	         Text c = new Text(splitInput[0]);
	         Text a = new Text(splitInput[1]);

	         Text opString = new Text("T"+c+","+a);
			 String c1= String.valueOf(1+(splitInput[0].hashCode()& Integer.MAX_VALUE % BNum));
		     String a1= String.valueOf(1+(splitInput[1].hashCode()& Integer.MAX_VALUE % BNum));
		     for(int i=1; i<= BNum; i++)
		         	{
		        	 	String b1= String.valueOf(i);
		        	 	reducerIndex= a1+b1+c1;
		        	 	System.out.println(reducerIndex);
		        	 	output.collect(new Text(reducerIndex), opString); 
					}      
	          //output.collect(new Text("1"), opString);
	     }
		
	}//TMapper Class
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
	{

		ArrayList < Text > RelS;
		ArrayList < Text > RelR;
		ArrayList < Text > RelT;

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{

			RelS = new ArrayList < Text >() ;
			RelR = new ArrayList < Text >() ;
			RelT = new ArrayList < Text >() ;
			
			while (values.hasNext()) 
			{
				String relationValue = values.next().toString();

				if (relationValue.indexOf('R') >= 0)
				{
					String finalVal = relationValue.substring(1, relationValue.length());
					RelR.add(new Text(finalVal));
				}//if
				if (relationValue.indexOf('T') >= 0)
				{
					String finalVal = relationValue.substring(1, relationValue.length());
					RelT.add(new Text(finalVal));

				}//if
				else 
				{
					String finalVal = relationValue.substring(1, relationValue.length());
					RelS.add(new Text(finalVal));
				}//else
			}//while

			for( Text r : RelR ) 
			{
				String[] Rtemp = r.toString().split(",");
				for (Text s : RelS) 
				{
					//String[] Rtemp = r.toString().split(",");
					String[] Stemp = s.toString().split(",");
					if(Rtemp[1].equalsIgnoreCase(Stemp[0]))
					{
						for (Text t : RelT) 
						{
							String[] Ttemp = t.toString().split(",");
							if(Stemp[1].equalsIgnoreCase(Ttemp[0]) && Ttemp[1].equalsIgnoreCase(Rtemp[0]))
							{
								output.collect(new Text(), new Text(Rtemp[0]+","+Stemp[0]+","+Ttemp[0]));
								//output.collect(new Text(), new Text(Rtemp[0]+","+Rtemp[0]+","+Stemp[1]+","+Ttemp[1]));
							}//if
						}//for
					}//if
					
					
					
				}//for
			}//for
		}//reduce

	}//Reduce Class

	public static class PartitionerClass implements org.apache.hadoop.mapred.Partitioner<Text, Text>
    {
		@Override
		public void configure(JobConf job) {// TODO Auto-generated method stub
			}
		
		//important for partitioning tuples with the same reducer ID to the same destination(partition)
      @Override
      public int getPartition(Text key, Text value, int numPartitions) 
      {
    	  double num=0;
    	  for (int i=0; i<key.getLength();i++)
           num = num+key.charAt(i)* Math.pow(10, (double) (key.getLength()-i));
    	  
        		  return ((int) num & Integer.MAX_VALUE) % numPartitions;
      }//fun getPartition

	
   
    }//class PartitionerClass
	
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception 
    {
    	long start = new Date().getTime();

        JobConf conf = new JobConf(MultipleInputs.class);
        conf.setJobName("OneWay Join2- Natural Join");

        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setJarByClass(OneWay2.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setPartitionerClass(PartitionerClass.class);
        conf.setReducerClass(Reduce.class);
       
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setNumReduceTasks(reducerNum);
        //conf.setNumReduceTasks(Integer.valueOf(args[4]));
        MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, RMapper.class);
        MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, SMapper.class);
        MultipleInputs.addInputPath(conf, new Path(args[2]), TextInputFormat.class, TMapper.class);
        FileOutputFormat.setOutputPath(conf, new Path(args[3]));
      	System.out.println("HEY Hey:"); 
        JobClient.runJob(conf);

    	System.out.println("HEY Yey:");
        long end = new Date().getTime();
        System.out.println("Job took "+(end-start) + "milliseconds");

    }//main

}//OneWay2 class

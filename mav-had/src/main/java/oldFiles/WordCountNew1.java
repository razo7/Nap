package oldFiles;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.util.Date;
import java.util.Enumeration;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountNew1 
{

  public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>
  {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      long start = new Date().getTime();
      int ct = 0;
      System.out.println("Mapper time is " + String.valueOf(start));
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) 
      {
    	ct++;
        word.set(itr.nextToken());
        context.write(word, one);
      }
      long end = new Date().getTime();
      System.out.println("Mapper finish time is " + String.valueOf(end- start) + " counting "+ String.valueOf(ct) );
    }//map
  }//MyMapper

  public static class dummyReducer extends Reducer<Text,IntWritable,Text,Text> 
  {
    
    public static final Log log = LogFactory.getLog(dummyReducer.class);

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
    {
    	String ip,ip2="gg";
       
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) 
            {
                NetworkInterface iface = interfaces.nextElement();// filters out 127.0.0.1 and inactive interfaces
                if (iface.isLoopback() || !iface.isUp())
                    continue;

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while(addresses.hasMoreElements()) 
                {
                    InetAddress addr = addresses.nextElement();
                    ip = addr.getHostAddress();//System.out.println(iface.getDisplayName() + " " + ip + ""key);
                    ip2 = ip.substring(11);
                    System.out.println("Reducer IP is " + ip2 + " for key: "+key +" and time is " + String.valueOf(new Date().getTime()));
                }//while
            }//while
       
      long start =  new Date().getTime();
      int sum = 0; 
      for (IntWritable val : values) 
           sum += val.get();
      
      long end = new Date().getTime();
      Text res = new Text (String.valueOf(sum)+"\t"+String.valueOf(end-start));
      Text word = new Text(ip2+"\t"+key);
      
      context.write(word, res); 
    }//reduce
  }//reducer
  public static class oldPartitionerClass extends Partitioner<Text, IntWritable>
  {		
		//important for partitioning tuples with the same reducer ID to the same destination(partition)
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) 
    {
  	  //double num=0;
  	  //for (int i=0; i<key.getLength();i++)
      //   num = num+key.charAt(i)* Math.pow(10, (double) (key.getLength()-i));
      //int res =((int) num & Integer.MAX_VALUE) % numPartitions;
  	  int res =(key.hashCode() & Integer.MAX_VALUE) % numPartitions; 
	  return res;
    }//fun getPartition

  }//class oldPartitionerClass
  

  public static void main(String[] args) throws Exception 
  {
	//https://github.com/saagie/example-java-read-and-write-from-hdfs/blob/master/src/main/java/io/saagie/example/hdfs/Main.java#L58
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, args[3]);
    job.setJarByClass(WordCountNew1.class);
    job.setMapperClass(MyMapper.class);
    //job.setCombinerClass(wcReducer.class);
    job.setPartitionerClass(oldPartitionerClass.class);  
    job.setReducerClass(dummyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(Integer.valueOf(args[2]));
   
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(new Path(args[1]),"out1"));  
    System.out.println(args[3]);
	  
	long start1 = new Date().getTime();
    if (!job.waitForCompletion(true))
    	System.exit(1);

    
    long end1 = new Date().getTime();
    long start2 = new Date().getTime();
    /*
    String hdfsuri = "hdfs://master:9000"; //hdfs getconf -confKey fs.default.name
    String path = "/user/hadoop2/";
    //String fileName = args[1];
    String fileName = args[1]+"/"+"out1/";
    
    conf.set("fs.defaultFS", hdfsuri);
    FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
  //Create a path
    Path newFolderPath = new Path(path);
    Path hdfsreadpath = new Path(newFolderPath + "/" + fileName);
    //Init input stream
    FSDataInputStream inputStream = fs.open(hdfsreadpath);
    //Classical input stream usage
    String out = IOUtils.toString(inputStream, "UTF-8");
    String line = out.toString();
	String[] splitInput = line.split("\\t");
	
    inputStream.close();
    System.out.println(out+" space "+splitInput[1]);
    fs.close();
    
    */
    Job job2 = Job.getInstance(conf, args[3]);
    job2.setJarByClass(WordCountNew1.class);
    job2.setMapperClass(MyMapper.class);
    //job2.setCombinerClass(IntSumReducer.class);
    job2.setPartitionerClass(newPartitionerClass.class); 
    job2.setReducerClass(dummyReducer.class);
//    job2.setReducerClass(wcReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    
    job2.setNumReduceTasks(Integer.valueOf(args[2]));
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(new Path(args[1]),"out2"));  
    
    if (!job2.waitForCompletion(true))
    	System.exit(1);
    long end2 = new Date().getTime();
    System.out.println("Job1 took "+(end1-start1) + " milliseconds");
    System.out.println("Job2 took "+(end2-start2) + " milliseconds");
    String hdfsuri = "hdfs://master:9000"; //hdfs getconf -confKey fs.default.name
    FileSystem  fs = FileSystem.get(URI.create(hdfsuri), conf);
    fs.close();
  }
  
  public static class wcReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
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
  }//wcReducer
  public static class newPartitionerClass extends Partitioner<Text, IntWritable>
  {
				//important for partitioning tuples with the same reducer ID to the same destination(partition)
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) 
    {
    	
  	  //double num=0;
  	  //for (int i=0; i<key.getLength();i++)
      //   num = num+key.charAt(i)* Math.pow(10, (double) (key.getLength()-i));
      //int res =((int) num & Integer.MAX_VALUE) % numPartitions;
  	  int res =(key.hashCode() & Integer.MAX_VALUE) % numPartitions;
      System.out.println("HEY OR!@");
    /*  String hdfsuri = "hdfs://master:9000"; //hdfs getconf -confKey fs.default.name

      String path = "/user/hadoop2/";
      String fileName1 = "output/res-wcEx1/out1/part-r-0000";
      Configuration conf = new Configuration();
       
      try {
		  FileSystem  fs = FileSystem.get(URI.create(hdfsuri), conf);
		  Path newFolderPath= new Path(path);
	      String fileName0 = fileName1 +String.valueOf(0); 
	  	  Path hdfsreadpath = new Path(newFolderPath + "/" + fileName0);
	      FSDataInputStream inputStream = fs.open(hdfsreadpath);
	      String exString = IOUtils.toString(inputStream, "UTF-8");
	      inputStream.close();
	      System.out.print(exString);
	      //fs.close();
	} catch (IOException e) 
      {// TODO Auto-generated catch block
		e.printStackTrace();}
      */
      int exclude=0 ,opt2 =0;
      if (res == exclude)
    	res = (res+1) % numPartitions;   
   	  
   	  return res;
    }//fun getPartition

   }//class newPartitionerClass

  
  
}//wordcount1

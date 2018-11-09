package oldFiles;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.util.Date;
import java.util.Enumeration;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle;



public class wcWithChange 
{
	 static String hdfsuri = "hdfs://master:9000"; //hdfs getconf -confKey fs.default.name
	 static String path = "/user/hadoop2/";
	 
	 public static String myIp ()
	    {
	    	String ip,ip2 = "";
	        Enumeration<NetworkInterface> interfaces;
			try 
			{
				interfaces = NetworkInterface.getNetworkInterfaces();
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
	                }//while
	            }//while
			}//try
			catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    return ip2;
	    }//myIp
	  public static String [] readORwriteHDFS (int readORwrite, String Folder, String File, String fileContent) throws IOException
	    {
	        Configuration conf = new Configuration();
	        conf.set("fs.defaultFS", hdfsuri);
	        FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
	        Path currFolderPath = new Path(path);
	        Path hdfsPath = new Path(currFolderPath + "/" + Folder); 
	        if (readORwrite == 1)
	        {	//read
	        	Path hdfsFile = new Path(hdfsPath + "/" + File);    
	        	FSDataInputStream inputStream = fs.open(hdfsFile);
		        //Classical input stream usage
		        String out = IOUtils.toString(inputStream, "UTF-8");
		        String line = out.toString();
		        String[] splitInput = line.split("\\s+");
		    	
		        inputStream.close();
		        System.out.println(out+" space "+splitInput[1]);
		        return splitInput;
	        }
	        else
	        {//write  
	       //     Path workingDir=fs.getWorkingDirectory();
	            
	            if(fs.exists(hdfsPath))  // If folder exists then delete and recreate         
	            {
	            	 fs.delete(hdfsPath, true);
	            	 fs.mkdirs(hdfsPath);   // Create folder if not exists
	            	 System.out.println("Create Directory");
	            }
	            else
	            	fs.mkdirs(hdfsPath);   // Create folder if not exists
	             Path hdfsFile = new Path(hdfsPath + "/" + File);    
	        	 FSDataOutputStream outputStream=fs.create(hdfsFile); //Classical output stream usage
		         outputStream.writeBytes(fileContent);
		         outputStream.close();
		         String[] errorSoon = new String[2];
		         errorSoon[0] = "Hello";
		         errorSoon[1] = "World";
		         return errorSoon;
	        }
	    }//readORwriteHDFS	
	 
  public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>
  {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
    	String ip = myIp();
      long start = new Date().getTime();
      //int ct = 0;
      System.out.println("Mapper time is " + String.valueOf(start));
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) 
      {
    	//ct++;// how many words in one line
        word.set(itr.nextToken());
        context.write(word, one);
      }
      long end = new Date().getTime();
      //System.out.println("Mapper finish time is " + String.valueOf(end- start) + " counting "+ String.valueOf(ct) );
      System.out.println("Mapper with IP " + ip+" and finish time is " + String.valueOf(end- start));
    }//map
  }//MyMapper

  public static class dummyReducer extends Reducer<Text,IntWritable,Text,Text> 
  { //public static final Log log = LogFactory.getLog(dummyReducer.class);
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {    
      String ip = myIp();                                             
      long start =  new Date().getTime();                                 
      int sum = 0; 
      for (IntWritable val : values) 
           sum += val.get();
      
      long end = new Date().getTime();
      Text res = new Text (String.valueOf(sum)+"\t"+String.valueOf(end-start));
      Text word = new Text(ip+"\t"+key);
      
      context.write(word, res); 
    }//reduce
  }//reducer
 
  public static class newPartitionerClass extends Partitioner<Text, IntWritable> implements  org.apache.hadoop.conf.Configurable
  {		
	  String bwString;
	  private static final Log LOG = LogFactory.getLog(newPartitionerClass.class);  
	  //important for partitioning tuples with the same reducer ID to the same destination(partition)
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) 
    {    	//read time
    	
     int res=0,time = 1;
     System.out.println("OR_Change-getPartition- No upload-2");
     LOG.info("OR_Change-getPartition- No upload-2");
  	 if (time==0)
  	  {
  		 res =(key.hashCode() & Integer.MAX_VALUE) % numPartitions; 
  	  }
  	 else
  	 {//when we have the new allocation
  		//String [] tempLinks = readORwriteHDFS(1,"Downlinks","downlinks","");		 
        String [] splitInput = bwString.split("\\s+");
        int [] downlinks = new int [splitInput.length];
        for (int i=0; i< splitInput.length; i++)
        	downlinks[i] = Integer.parseInt(splitInput[i]);
        
     	 int W = 0 ;//sum downlinks
     	    for (int i =0; i<numPartitions; i++)
     	    W += downlinks[i];
     	//      W += downlinks_list.get(i);
     	    res =(key.hashCode() & Integer.MAX_VALUE) % W;

     	    int optPartit = 0;
     	    int partitionIndicator = downlinks[0];
     	   // int partitionIndicator = downlinks_list.get(0);
     	    while (res > partitionIndicator)
     	         {
     		         optPartit++;        
     		         partitionIndicator += downlinks[optPartit];
     		//         partitionIndicator += downlinks_list.get(optPartit);
     	         }//while
     	    res = optPartit;
     	 }//else

  	 
	  return res;
    }//fun getPartition

    @Override
    public void setConf (Configuration conf)
    {
    	bwString = conf.get("bw");
    }
    
    @Override
    public Configuration getConf()
    {
    	return null;
    }

	
}//class newPartitionerClass
    
  public static void main(String[] args) throws Exception 
  {//https://github.com/saagie/example-java-read-and-write-from-hdfs/blob/master/src/main/java/io/saagie/example/hdfs/Main.java#L58
	  //conf.set("fs.defaultFS", hdfsuri); //can be used be context
      //FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);//write to hdfs
    //  readORwriteHDFS(0, "Downlinks","downlinks", args[3]);
//		                                                                setting up configuration                     
	  Configuration conf = new Configuration();
      // # of mappers = size_input / split size [Bytes], split size= max(mapreduce.input.fileinputformat.split.minsize, min(mapreduce.input.fileinputformat.split.maxsize, dfs.blocksize))
      conf.set("mapreduce.input.fileinputformat.split.minsize", args[2]); //The minimum size chunk that map input should be split into. Note that some file formats may have minimum split sizes that take priority over this setting.
      conf.set("mapreduce.input.fileinputformat.split.maxsize", args[2]); //The maximum size chunk that map input should be split into.
      conf.set("bw", args[4]); //pass the downlink vector of partitions

//      								                                       MR1
	Job job = Job.getInstance(conf, args[5]);
    job.setJarByClass(wcWithChange.class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(dummyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(Integer.valueOf(args[3]));
   
    //MultipleInputs.addInputPath(job, path, inputFormatClass, mapperClass);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(new Path(args[1]),"out1"));  
    System.out.println("Downlinks list: "+args[4]);
	  
	long start1 = new Date().getTime();
    if (!job.waitForCompletion(true))
    	System.exit(1);  
    long end1 = new Date().getTime();
//	                                                                                 MR2
    long start2 = new Date().getTime();
    
    Job job2 = Job.getInstance(conf, args[6]);
    job2.setJarByClass(wcWithChange.class);
    job2.setMapperClass(MyMapper.class);
    job2.setPartitionerClass(newPartitionerClass.class); 
    job2.setReducerClass(dummyReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    job2.setNumReduceTasks(Integer.valueOf(args[3]));
   
    
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(new Path(args[1]),"out2"));  
    
    if (!job2.waitForCompletion(true))
    	System.exit(1);
    long end2 = new Date().getTime();
    System.out.println("Job1 took "+(end1-start1) + " milliseconds");
    System.out.println("Job2 took "+(end2-start2) + " milliseconds");

    // fs.close();//close opened Hadoop File System
  }
  

  
}//wordcount1

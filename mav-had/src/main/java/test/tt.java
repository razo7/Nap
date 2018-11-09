package test;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Enumeration;

import org.apache.hadoop.io.Text;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class tt
{
	public static void main(String[] args) throws Exception 
    {
    
    Text a = new Text("a");
    Text b = new Text("b");
    int hash1 = a.hashCode();
    int hash2 = b.hashCode();
    System.out.println(hash1+" "+hash2);
    long start1 = new Date().getTime();
    long start2 = new Date().getTime();
    System.out.println(String.valueOf(start1)+" "+String.valueOf(start2));
    
    String hdfsuri = "hdfs://master:9000"; //hdfs getconf -confKey fs.default.name

    String path = "/user/hadoop2/";
    String fileName1 = "output/wc3/out1/part-r-0000";
    String fileName2 = "output/wc3/out2/part-r-0000";
    int numRed = 6;
    String outs1 [][] = new String[numRed][];
    String outs2 [][] = new String[numRed][];
    int reducerMapping1 [][] = new int [numRed][4];
    int reducerMapping2 [][] = new int [numRed][4];
    
    // ====== Init HDFS File System Object
    Configuration conf = new Configuration();
   
    conf.set("fs.defaultFS", hdfsuri); // Set FileSystem URI
   
    // Set HADOOP user
    System.setProperty("HADOOP_USER_NAME", "hadoop2");
    System.setProperty("hadoop.home.dir", "/");
    //Get the filesystem - HDFS
    FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

    //==== Create folder if not exists
   // Path workingDir=fs.getWorkingDirectory();
    Path newFolderPath= new Path(path);
   // if(!fs.exists(newFolderPath)) {
       // Create new Directory
    //   fs.mkdirs(newFolderPath);
    // }

    //==== Write file
    //Create a path
    //Path hdfswritepath = new Path(newFolderPath + "/" + fileName);
    //Init output stream
    //FSDataOutputStream outputStream=fs.create(hdfswritepath);
    //Cassical output stream usage
   // outputStream.writeBytes(fileContent);
   // outputStream.close();
 

    //==== Read file
 
    System.out.println("\nNormal Partitioning");
    for (int i=0; i<numRed; i++)
    {
    	String fileName0 = fileName1 +String.valueOf(i); 
    	Path hdfsreadpath = new Path(newFolderPath + "/" + fileName0);
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        String exString = IOUtils.toString(inputStream, "UTF-8").replace("\n", "\t");
        inputStream.close();
        String line = exString.toString();
        outs1[i] = line.split("\\t");
        //System.out.println("\n"+String.valueOf(outs1[i].length/4));
        if ( !exString.isEmpty())
        {    System.out.println("\nReducer #"+String.valueOf(i)+" and IP "+outs1[i][0] + " with outputs");
            // for (int j=0;j< outs1[i].length; j++)
        	//    System.out.print(" "+outs1[i][j]);             
             for (int j=1;j< outs1[i].length; j+=4)
	            System.out.print(" "+outs1[i][j]);     
             reducerMapping1[i][0] = outs1[i].length /4;
             reducerMapping1[i][2] = Integer.parseInt(outs1[i][0]);
             for (int k=0;k<outs1[i].length /4 ; k++)
            	   	 reducerMapping1[i][1] += (Integer.parseInt(outs1[i][4*k+3]) == 0) ? 0 : (Integer.parseInt(outs1[i][4*k+2])/Integer.parseInt(outs1[i][4*k+3]));                  
        }     
    }
    System.out.println("\nUpdated Partitioning");
    for (int i=0; i<numRed; i++)
    {
    	String fileName0 = fileName2 +String.valueOf(i); 
    	Path hdfsreadpath = new Path(newFolderPath + "/" + fileName0);
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        String exString = IOUtils.toString(inputStream, "UTF-8").replace("\n", "\t");
        inputStream.close();
        String line = exString.toString();
        outs2[i] = line.split("\\t");
        //System.out.println("\n"+String.valueOf(outs2[i].length/4));
        if ( !exString.isEmpty())
        {    System.out.println("\nReducer #"+String.valueOf(i)+" and IP "+outs2[i][0] + " with outputs");
             //for (int j=0;j< outs2[i].length; j++)
        	 //   System.out.print(" "+outs2[i][j]);
             for (int j=1;j< outs2[i].length; j+=4)
    	        System.out.print(" "+outs2[i][j]);      
             reducerMapping2[i][0] = outs2[i].length /4;
             reducerMapping2[i][2] = Integer.parseInt(outs2[i][0]);
             for (int k=0;k<outs2[i].length /4 ; k++)
            	   	 reducerMapping2[i][1] += (Integer.parseInt(outs2[i][4*k+3]) == 0) ? 0 : (Integer.parseInt(outs2[i][4*k+2])/Integer.parseInt(outs2[i][4*k+3]));
                  
        }
    }
   
    
    System.out.println("\nNormal Partitioning\nReducer# #keys|avg_time|prev_ip|next_ip");
    for (int i=0; i<reducerMapping1.length; i++)
       { 
    	System.out.printf("%d ",i);
    	   for (int j=0; j<reducerMapping1[i].length; j++)
    		System.out.printf("%d|", reducerMapping1[i][j]);
    	   System.out.println();
       }
   
    System.out.println("Updated Partitioning\nReducer# #keys|avg_time|prev_ip|next_ip");
    for (int i=0; i<reducerMapping2.length; i++)
      { 
	   System.out.printf("%d ",i);
	   for (int j=0; j<reducerMapping2[i].length; j++)
	   		System.out.printf("%d|", reducerMapping2[i][j]);  
	   System.out.println();
       }
   
    
    fs.close();
    
   /*
      String ip;
    try {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();
            // filters out 127.0.0.1 and inactive interfaces
            if (iface.isLoopback() || !iface.isUp())
                continue;

            Enumeration<InetAddress> addresses = iface.getInetAddresses();
            while(addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                ip = addr.getHostAddress();
                String ip2 = ip.substring(11);
                System.out.println("\n"+iface.getDisplayName() + " hey  " + ip+ " " +ip2);
            }
        }
    } catch (SocketException e) {
        throw new RuntimeException(e);
    }
    */

   
   
    }

}

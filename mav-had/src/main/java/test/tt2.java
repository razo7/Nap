package test;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class tt2
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
	 
	public static void main(String[] args) throws Exception 
    {
		String text = "P329143	BJ\\_amp\\_#248;rn	Myhrhaug";
		String[] splitInput = text.split("\\t");


		System.out.println(splitInput[0] +" WOW " + splitInput[1] +" WOW " + splitInput[2] );
		/*splitInput[2
		List<Integer> downlinks_list = new ArrayList<>();
		 String[] splitInput = "1 2 3 5".split("\\s+");
	     for (int i=0; i< splitInput.length; i++)
	     {
	    	 downlinks_list.add(Integer.parseInt(splitInput[i]));
	    	 System.out.println( downlinks_list.get(i));
	     }
	    
	    
		 Configuration conf = new Configuration();
		 System.out.println(myIp());
	      conf.set("fs.defaultFS", hdfsuri);
	      FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
	      readORwriteHDFS(0, "Downlinks","downlinks", "122 8 3 5 2");
	      System.out.println(myIp());
	      readORwriteHDFS(1,"Downlinks","downlinks","");
	      fs.close();//close opend Hadoop File System
	      */
    }

}

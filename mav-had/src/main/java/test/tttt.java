package test;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
public class tttt {
private final static Random rand = new Random();
	public static void main(String[] args){
		// TODO Auto-generated method stub
		String a = "0";
		String b = "1";
		String res = a+b;
		//System.out.println(("0"+"0").hashCode() + " " + ("0"+"1").hashCode() + " " + ("1"+"0").hashCode() + " " + ("1"+"1").hashCode() );
		int A=5;
		int B=5;
		for (int i=0; i< A; i++)
		{
			String a1 = String.valueOf(i);
			for (int j=0; j< B; j++)
			{
				String b1 = String.valueOf(j);
                String res3 = a1+b1;
				//System.out.println(res3 +": "+ res3.hashCode()  );
			}}
		//System.out.println(closeNum(19));
		int r = 5;
		int W = 7;
		int x = Math.round(2*(float)W/r);
		//System.out.println(x);
		int [][] indexReducerBySlave = new int [3][8];
	     int [] countReducerBySlave = {0,0,0};
	     ++countReducerBySlave[0];
	    
	     indexReducerBySlave[0][0] = 2;
	 	//System.out.println("a " +": "+ "a ".hashCode()  );
	     List <Integer> Rel1 = new ArrayList<Integer>();
	     List <Integer> Rel2 = new ArrayList<Integer>();
	     List <Integer> Rel3 = new ArrayList<Integer>();
	     int num = 7;
	     for (int i = 0; i < 10000; i++)
	     {
	    	 String s = String.valueOf(i);
	    	 Text t = new Text (s);
	    	 int h = t.hashCode()  & Integer.MAX_VALUE;
	    	 int m = h % num;
	    	 if (m <= 3)
	       		 Rel1.add(i);
	    	 else if(m <= 5)
	    		 Rel2.add(i);
	    	 else
	    		 Rel3.add(i);
	     }
	   System.out.println("R1:"+ String.valueOf(Rel1.size())  +"\n\n");
	   //  for (Integer r1 : Rel1 )
	   // 	 System.out.println(String.valueOf(r1));
	
	     System.out.println("R2: "+ String.valueOf(Rel2.size())  +"\n\n");
	  //   for (Integer r2 : Rel2 )
	  //  	 System.out.println(String.valueOf(r2));
	     System.out.println("R3:"+ String.valueOf(Rel3.size())  +"\n\n");
	  //   for (Integer r3 : Rel3 )
	  //  	 System.out.println(String.valueOf(r3));
	String ss = "1 2 3 :4 5 6 :6 7";
	String [] s1 = ss.split(":");
	String [] s2 = s1[0].split("\\s+");
	     System.out.println(s2[2]);
	     
	     //System.out.println(s + ", hash= " + String.valueOf(h));    
     }
	public static int closeNum (int num)
	{
		int cand = (int) Math.floor(Math.sqrt(num));
		while (cand > 0)
		{
			if (num % cand == 0)
				break;
			cand--;
		}
		return cand;
		
	}
}

package test;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;

import examples.TextPair;
public class tttt {
private final static Random rand = new Random();
public static int newHash (String s )
{
	long hash = 1;
	for ( int i = 0; i < s.length(); i++)
		hash = hash *33 + (int) s.charAt(i)*131;			
	return (int) hash & Integer.MAX_VALUE;
}
public static int MJHashEqual (String mykey, int ANum)
{
	 return Character.getNumericValue(mykey.charAt(0)) * ANum + Character.getNumericValue(mykey.charAt(1)); 
	 
}//MJHashEqual
	public static void main(String[] args){
		// TODO Auto-generated method stub
		String a = "0";
		String b = "1";
		String res = a+b;
		//System.out.println(("0"+"0").hashCode() + " " + ("0"+"1").hashCode() + " " + ("1"+"0").hashCode() + " " + ("1"+"1").hashCode() );
		int res1,res2,res4, A=3;
		int B=3;
		int size = A *B;
		int arr1 [] = new int[size];
		int arr2 [] = new int[size];
		int arr4 [] = new int[size];
		for (int i =0 ; i < size; i++)
			arr1 [i] = arr2[i] = arr4 [i]= 0;
		for (int i=0; i< A; i++)
		{
			String a1 = String.valueOf(i);
			for (int j=0; j< B; j++)
			{
				String b1 = String.valueOf(j);
                String res3 = a1+b1;
                res1 = res3.hashCode() % size;
                res2 = newHash(res3) % size;
                res4 = MJHashEqual(res3,A) % size;
                arr1[res1]++;
                arr2[res2]++;
                arr4[res4]++;
                System.out.println(String.valueOf(res3));
        		System.out.print("hashcode- "+ " : "+ String.valueOf(res1) );
                System.out.print(", newHash- " + ": "+ String.valueOf(res2)  );
                System.out.print(", MJHashEqual- "+": "+ String.valueOf(res4) +"\n" );
			}}
		System.out.println("");
		for (int i =0 ; i < size; i++)
			System.out.print(arr1 [i]);
		System.out.println("");
		for (int i =0 ; i < size; i++)
			System.out.print(arr2 [i]);
		System.out.println("");	
		for (int i =0 ; i < size; i++)
			System.out.print(arr4 [i]);
		System.out.println("");	
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
	     int [] dd = new int [2];
	     dd[0] = 0;
	     dd[1] = 0;
	     int [] dd2 = {8,3};
	String ss = "4 3:6 8";
	String [] s1 = ss.split(":");
	for ( int i =0; i< s1.length; i++)
	{
		for ( int j =0; j< s1.length; j++)
		{
			String [] s2 = s1[j].split("\\s+");
			dd[i] += (Integer.parseInt(s2[i])* dd2[i]); 
		}
		 System.out.println(dd[i]/s1.length);
	}
	
	    
	     
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

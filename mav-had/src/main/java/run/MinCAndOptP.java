package run;
public class MinCAndOptP 
{
    public static void main(String[] args) throws Exception
    {
        double [] f = new double []{4, 2 ,1};//Assumption:  array is in descending/ asecending order
        double [][]  res =  new double[2][f.length];;
        double s = 2; //joint attributes, 2 for ACM example
        double Bc_param = 1; // s* Math.pow(x*y*z, 1/s) or for ACM example y+2*Math.pow(x*z, 2) 
        res = OptPartit(f, s, Bc_param);
        printResult(f, s, res);
    }//main
    public static double [][] OptPartit(double [] f, double s,  double Bc_param)
    {//Go over (W -f.length) candidates for the optimal partition for any amount of reducers
        int W = 0;
        double [] lambda = new double[f.length];//optimal partition for any amount of reducers
        for (int j = 0; j < f.length; j++)
        {
        	W+= f[j];//Total bandwidth 
        	lambda[j] = f[j];// start with partition f
        }
        double C = Math.pow((double) W , -(s-1)/s);//optimal completion time
        double [][] tempArrays = new double[2][f.length];// temporarily list of optimal partition and optimal C
        tempArrays[0][0] = 1;//start with partition of one reducer to the machine with the best link
        for (int i = 2; i <= (W -f.length); i++)
        {
            tempArrays = OptC(f, s, tempArrays[0]);
            if (tempArrays[1][0] <= C)//using one more reducer, does it has a better balancing of the load?
            {
                lambda = tempArrays[0] ;
                C = tempArrays[1][0] ;
            }//if
        }//for
        tempArrays[0] = lambda;
        tempArrays[1][0] = C * Bc_param;
        return tempArrays ;
    }//OptPartit

    public static double [][] OptC (double [] f, double s, double [] tempP )
    {//Go over f.length candidates for the optimal partition given amount of reducers
        double [][] res = new double[2][f.length];//temporarily list of optimal partition and optimal C
        int v = 0;
        double [] lambda = new double[tempP.length];//optimal partition given amount of reducers
        for (int j = 0; j < tempP.length; j++)
        {
            lambda[j] =  tempP[j];
            v+= tempP[j];
        }
        lambda[0]++;//start with old partition and add one reducer to the machine with the best link
        double ff = Math.pow(v+1, -(s-1)/s);
        double C = ff*  (tempP[0]+1) / f[0];
        for ( int i = 1 ; i < f.length; i++)
        {
            double tempC = ff* (tempP[i]+1) / f[i];
            if (tempC < C )//partitioning one reducer to another machine, does it has a better balancing of the load?
            {
                lambda = tempP;
                lambda[i]++;
                C = tempC;
            }//if
        }//for
        res[0] = lambda;
        res[1][0] = C;
    return  res;
    }//OptC

    public static void printResult (double [] f, double s, double [][] res)
    {
    	System.out.print("Optimal Partition for " + f.length  + " machines with downlink vector {");
        for (int i =0; i< f.length; i++)
            System.out.print( (int)f[i] +  " ");
        System.out.print("} and " + (int)s + " joint attributes :\n{");
        double r = 0;
        for (int i =0; i< res[0].length; i++)
        {
            System.out.print((int)res[0][i] + " ");
            r +=res[0][i];
        }
        System.out.println("} with " + (int)r + " reducers and completion time " + res[1][0]);
    }//printResult
}//MinCAndOptP

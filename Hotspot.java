package com.dds.phase3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Hotspot {
	
	static int totalNumber = 71176 ; 
	static double latitude_min = 40.50 ;
	static double longitude_min = 73.70 ;
	static double latitude_max = 40.90 ;
	static double longitude_max = 74.25 ;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		 	final SparkConf conf = new SparkConf().setAppName("giscup") ; 
		 	final JavaSparkContext sc = new JavaSparkContext(conf);
		 	
		 	try{
			 	HashMap<String,Integer> cells = cellCreation(sc,args);
			 	TreeMap<Double,String> topZscore = getisOrd(cells);
			 	outputFile(args , topZscore); 
		 	}
		 	catch(Exception e){
		 		e.printStackTrace();
		 	}
					
		
	}
	private static double calculateS(HashMap<String,Integer> cells,double mean) {
		double temp = 0.00;
		Iterator it = cells.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry me = (Map.Entry) it.next();
			temp += Math.pow(((Integer)me.getValue()).doubleValue(),(double)2);
		}
		
    	temp /= (double) totalNumber;
    	temp -= Math.pow(mean, (double)2);
		return Math.sqrt(temp);
	}
	
	private static void outputFile(String[] args, TreeMap<Double,String> topZscore) throws IOException {
		// TODO Auto-generated method stud
		FileWriter fw = new FileWriter(args[1]);
		BufferedWriter bw = new BufferedWriter(fw);
		
		int count = 0;
    	String entry = "";
    	StringBuilder sb = new StringBuilder();
    	Iterator it = topZscore.entrySet().iterator();
    	while(it.hasNext()&& count<50){

    		Map.Entry me = (Map.Entry) it.next() ; 
    		entry = (String) me.getValue();
    		sb.append((Integer.parseInt(entry.split("%")[2]) + (Hotspot.latitude_min*100.0))/100.0);
    		sb.append(',');
    		sb.append((-1*(Integer.parseInt(entry.split("%")[3]) + (Hotspot.longitude_min*100.0)))/100.0);
    		sb.append(',');
    		sb.append(Integer.parseInt(entry.split("%")[1]));
    		sb.append(',');
    		sb.append(entry.split("%")[0]);
    		sb.append('\n');
    		
    		count++;
    	}
    	bw.write(sb.toString());
        bw.close();
        System.out.println("Written Data to an output file");
	}

	private static double mean(HashMap<String,Integer> cells) {
		// TODO Auto-generated method stub
		System.out.println("In mean");
		int sum=0;
		Iterator it = cells.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry me = (Map.Entry) it.next();
			sum += (int) me.getValue();
			
		}
		
		return (double)sum/(double)totalNumber;
	}
	private static TreeMap<Double,String> getisOrd(HashMap<String,Integer> cells) {
		// TODO Auto-generated method stub
		System.out.println("Calling TopZScore function");
	
   	 TreeMap<Double, String> topZscore = new TreeMap(Collections.reverseOrder());
		double getis = 0.0 ; 
		double xBar = mean(cells);
		double sValue = calculateS(cells, xBar);
		System.out.println("Mean" + xBar);
		System.out.println("Svalue" + sValue);
		Iterator it = cells.entrySet().iterator();
			while(it.hasNext()){
			Map.Entry me = (Map.Entry) it.next();
			String row= (String) me.getKey();
			String a[] = row.split("#");
			getis =calculateGetis(cells,xBar,Integer.parseInt(a[0]),Integer.parseInt(a[1]),Integer.parseInt(a[2]),sValue);
			//if(!((Double)getis).isNaN())
				topZscore.put(getis,Double.toString(getis)+"%"+a[0]+"%"+a[1]+"%"+a[2]) ;
			}
	System.out.println("TopZScore function complete");
	return topZscore ; 
	} 
static double calculateGetis(HashMap<String,Integer> cells, double xBar,int time, int row, int col,double sValue){
    	
    	int count = 0;
    	double num = 0.00,den=0.0;
		 
    			 for(int i=-1;i<=1;i++)
    				 for(int j=-1;j<=1;j++)
    					 for(int k=-1;k<=1;k++){
    							 int a = time + i ;
    							 int b = row + j ;
    							 int c = col + k ; 
    							 String key =  a + "#" + b + "#" + c ; 
    							 if(cells.containsKey(key)){
    							 num+= cells.get(key);
    							 count++;
    						 }	 
    					 }	 
   			 num -= ((xBar)*(double)count);

    	den = (double)totalNumber *(double)count;
    	den -= Math.pow((double) count, (double) 2);
    	den /= (double) (totalNumber-1);
    	den = Math.sqrt(den);
    	den *= sValue;
    	
		return num/den;
	}
	private static HashMap<String,Integer> cellCreation(JavaSparkContext sc, String[] args) throws IOException {
		
		System.out.println("In cell creation function");
		JavaRDD<String> textFile = sc.textFile(args[0]);
		
		HashMap<String,Integer> cells = new HashMap<String,Integer>();
    	JavaPairRDD<String, Integer> mapPairs = textFile.mapToPair(new MapPair());
    	JavaPairRDD<String, Integer> pairCounts = mapPairs.reduceByKey(new ReduceKey()) ; 
    	
    	List<Tuple2<String,Integer>> collectedData = pairCounts.collect();
    	String[] row = new String[3];
    	String key = null; 
    	for(int i=0;i<collectedData.size();i++){
    		if(!collectedData.get(i)._1.equals("OutOfEnvelope")){
    		row = collectedData.get(i)._1.split("%");
    		key = row[0] + "#" + row[2]+"#" + row[1] ; 
    			cells.put(key, collectedData.get(i)._2);
        	}
  
    	}	
    	return cells ; 
	}
}

	class MapPair implements PairFunction<String, String, Integer> {
		  @Override
		  public Tuple2<String, Integer> call(String file) {
			 try{
			  String[] row = file.split(",");
			Double longitude = Double.parseDouble(row[5]);
	  		Double latitude = Double.parseDouble(row[6]);
	  		int day = Integer.parseInt(row[1].split(" ")[0].split("-")[2])-1;
	  		int lat,lon;
	  		if(latitude >= Hotspot.latitude_min && latitude <=Hotspot.latitude_max && longitude >= -Hotspot.longitude_max && longitude <= - Hotspot.longitude_min) {
	  			lat = indexing(latitude);
	      		lon = indexing(longitude);
	 
	      		return new Tuple2<String, Integer>(day+"%"+lon+"%"+lat, 1); 
	  		} }
			 catch(Exception e){
				 return new Tuple2<String, Integer>("OutOfEnvelope", 1); 
			 }
			  
			  return new Tuple2<String, Integer>("OutOfEnvelope", 1); 
	}
		  
		  public int indexing(Double point){
		    	int index;
				if(point< 0){
					point = Math.abs(point);
					index = (int) Math.ceil(point*100.0);
				}
				else
					index = (int) Math.floor(point*100.0);
				
				
				if(index<= Hotspot.latitude_max*100.0 && index>= Hotspot.latitude_min*100.0)
					return (int) (index- Hotspot.latitude_min*100.0);

				else
					return (int) (index-Hotspot.longitude_min*100.0);
		    	
		    }
	}
	
	class ReduceKey implements Function2<Integer,Integer,Integer> {	  
		public Integer call(Integer a, Integer b) {
			  return a + b; 
		}
	}
	
	




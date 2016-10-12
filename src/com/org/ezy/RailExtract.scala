package com.org.ezy


import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;

object RailExtract {
  def main(args:Array[String]){
    // Setting up Spark Context might vary, depending on the execution environment.
    val conf = new SparkConf()
    .setAppName("RailDataApp")
    .setMaster("local[*]")
    val sc = new SparkContext(conf)
  
    
    /*
     Text File is presented in the CSV format.
     We are filtering the header first.
     Sample data in the CSV file is as below.
    	  '00567',BNC SUVIDHA SPL,2,BAM ,BRAHMAPUR      ,'01:10:00','01:12:00',166,BBS ,BHBB    ,BNC ,BANGALORE CANT 
				'00567',BNC SUVIDHA SPL,3,VSKP,VISAKHAPATNAM  ,'05:10:00','05:30:00',443,BBS ,BHBB    ,BNC ,BANGALORE CANT 
				'00567',BNC SUVIDHA SPL,4,BZA ,VIJAYAWADA JN  ,'11:10:00','11:20:00',793,BBS ,BHBB    ,BNC ,BANGALORE CANT 
				'00567',BNC SUVIDHA SPL,5,RU  ,RENIGUNTA JN   ,'16:42:00','16:52:00',1169,BBS ,BHBB    ,BNC ,BANGALORE CANT 
				'00567',BNC SUVIDHA SPL,6,JTJ ,JOLARPETTAI    ,'20:35:00','20:37:00',1367,BBS ,BHBB    ,BNC ,BANGALORE CAN
	 		*/
    
    // Make sure the file exists in the path provided to sc.textFile(...)
    val textFile = sc.textFile("isl1.csv").zipWithIndex().filter(_._2>0)
  
    // The RDD is being filtered on the basis of 4th column present in a row.
    // In the below example we are passing the value MKU, 
    // splitData will contain all the rows having 4th column as "MKU"
    val splitData = textFile.filter { line => line._1.split(",").transform { x => x.trim()}(3).equals("BZA")}
    
    println("Total number of records:"+splitData.count) 
    println("And the records are....")
    splitData.foreach { x => println(x) }
   

  }
}
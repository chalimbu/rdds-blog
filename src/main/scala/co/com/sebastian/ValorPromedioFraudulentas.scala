package co.com.sebastian

import org.apache.log4j._
import org.apache.spark._

object ValorPromedioFraudulentas {

  def main(arg: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","ValorPromedio")

    val input=sc.textFile("data/archive/synthetic_financial_data.csv")
    val skipable_first_row = input.first()
    val useful_csv_rows = input.filter(row => row != skipable_first_row)

    val isFraudulentAndValue=useful_csv_rows.map(x=>{
      val fields= x.split(",")
      (fields.apply(5).toInt==1,fields.apply(3).toFloat)
    })
    val onlyFraudulent=isFraudulentAndValue.filter(x=>x._1==true)
    val averageFraudulent=onlyFraudulent.mapValues(x=>(1,x)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>x._2/x._1)

    averageFraudulent.collect.map(println)

  }

}

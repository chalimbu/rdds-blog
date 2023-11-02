package co.com.sebastian

import org.apache.log4j._
import org.apache.spark._

object PromedioOperacionPorClient {
  def main(arg: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PromedioPorCliente")

    val input = sc.textFile("data/archive/synthetic_financial_data.csv")
    val skipable_first_row = input.first()
    val useful_csv_rows = input.filter(row => row != skipable_first_row)

    val clientIdAndValue = useful_csv_rows.map(x => {
      val fields = x.split(",")
      (fields.apply(1), fields.apply(3).toFloat)
    })
    val promedioOperacionPorClient=clientIdAndValue.mapValues(x=>(1,x)).reduceByKey((op1,op2)=>(op1._1+op2._1,op1._2+op2._2)).mapValues(x=>x._2/x._1)

    promedioOperacionPorClient.collect.map(println)

  }
}

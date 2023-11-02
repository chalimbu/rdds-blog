package co.com.sebastian

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FraudulentByAge {
  object GruposEdadesMenorA extends Enumeration {
    type GruposEdadesMenorA = Value

    val Veinte,Cuarenta,Sesenta,Ochenta,Cien,Mas = Value
  }


  def main(arg: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FraudolentosPorEdad")

    val input = sc.textFile("data/archive/synthetic_financial_data.csv")
    val skipable_first_row = input.first()
    val useful_csv_rows = input.filter(row => row != skipable_first_row)

    val ageFraudulent = useful_csv_rows.map(x => {
      val fields = x.split(",")
      (fields.apply(9).toInt, fields.apply(5).toInt==1)
    })
    val sumFraudulentByAge=ageFraudulent.filter(x=>x._2).mapValues(x=>1).reduceByKey((op1,op2)=>(op1+op2))

    val generatingGroups=sumFraudulentByAge.map(x=>(x._2,x._1)).mapValues(x=>{
      x match {
        case x if x<20 => GruposEdadesMenorA.Veinte
        case x if x<40 => GruposEdadesMenorA.Cuarenta
        case x if x<60 => GruposEdadesMenorA.Sesenta
        case x if x<80 => GruposEdadesMenorA.Ochenta
        case x if x<100 => GruposEdadesMenorA.Cien
        case _ => GruposEdadesMenorA.Mas
      }
    }).map(x=>(x._2,x._1)).reduceByKey((x,y)=>(x+y)).map(x=>(x._2,x._1)).sortByKey()

    generatingGroups.collect.map(println)

  }

}

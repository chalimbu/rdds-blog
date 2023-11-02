package co.com.sebastian

import org.apache.log4j._
import org.apache.spark._

object TotalTransacionesCliente {

  def main(arg: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Totaltransacionescliente")

    val input = sc.textFile("data/archive/synthetic_financial_data.csv")
    val skipable_first_row = input.first()
    val useful_csv_rows = input.filter(row => row != skipable_first_row)
    //https://itecnote.com/tecnote/scala-remove-first-row-of-spark-dataframe/
    val clientWithAmountSpent = useful_csv_rows.map(x => {
      val fields = x.split(",")
      (fields.apply(1), fields.apply(3).toFloat)
    })

    val result = clientWithAmountSpent.reduceByKey((a, b) => a + b)

    result.collect.map(println)

  }

}

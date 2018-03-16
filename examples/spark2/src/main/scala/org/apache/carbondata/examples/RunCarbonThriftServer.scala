package org.apache.carbondata.examples

/**
  * Created by root on 3/16/18.
  */
object RunCarbonThriftServer {
  def main(args: Array[String]): Unit = {
    println("start")
    CarbonThriftServer.main(args)
    println("end")
  }
}



import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.reflect.io.File

object MutualDetails {
  def main(args: Array[String]) {
  val conf = new SparkConf().setAppName("MutualDetails").setMaster("local");
  val sc = new SparkContext(conf)
  if(args.length < 4){
    println("Enter proper number of args")
    exit(2);
  }
  val txtFile = sc.textFile(args(0))
  val userIdList = txtFile.map(line => (line.split("\t")(0), (if (line.split("\t").length > 1) {line.split("\t")(1).split(",").toList} else {List()})))
  val user1List = userIdList.filter(userId => (userId._1 == args(2) || userId._1 == args(3))).collect
  val keyOfList = user1List(0)._1.concat(",").concat(user1List(1)._1)
  val result = user1List(0)._2.intersect(user1List(1)._2)
  
  val userDetail = sc.textFile(args(1))
  val userNameZip = userDetail.map(x => (x.split(",")(0), ( x.split(",")(1)+":"+x.split(",")(6)) ))
  var resultDetails = List[String]()
  for( x <- result if result.length > 0){
       val userNameZ = userNameZip.filter(z=>(z._1 == (x))).collect.toList
       resultDetails =  userNameZ(0)._2 :: resultDetails
  }
  println("Mutual Friends Details: %s  [%s]".format(keyOfList, resultDetails mkString ","))
  
  var resMap:Map[String,String] = Map()
  resMap += ( keyOfList -> resultDetails.mkString(","))
  sc.parallelize(resMap.toSeq).saveAsTextFile(args(4))
  }
}
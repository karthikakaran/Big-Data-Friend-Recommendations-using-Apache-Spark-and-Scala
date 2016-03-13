

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Mutual {
  def main(args: Array[String]) {
 	val conf = new SparkConf().setAppName("Mutual").setMaster("local");
  val sc = new SparkContext(conf)
  if(args.length < 4){
    println("Enter proper number of args")
    exit(2);
  }
  val txtFile = sc.textFile(args(0))
  val userIdList = txtFile.map(line => (line.split("\t")(0), (if (line.split("\t").length > 1) {line.split("\t")(1).split(",").toList} else {List()})))
  val user1List = userIdList.filter(userId => (userId._1 == args(1) || userId._1 == args(2))).collect
  //user1List.foreach(println)
  val keyOfList = user1List(0)._1.concat(",").concat(user1List(1)._1)
  val result = user1List(0)._2.intersect(user1List(1)._2)
  println("Mutual Friends: %s  %s".format(keyOfList, result mkString ","))
  
  var resMap:Map[String,String] = Map()
  resMap += ( keyOfList -> result.mkString(","))
  sc.parallelize(resMap.toSeq).saveAsTextFile(args(3))
  }
}


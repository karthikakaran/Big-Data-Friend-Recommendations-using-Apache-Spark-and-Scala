import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object AgeAvg {
  def main(args: Array[String]) {
  val conf = new SparkConf().setAppName("AgeAvg").setMaster("local");
  val sc = new SparkContext(conf)
  val txtFile = sc.textFile(args(0))
  val userIdList = txtFile.map(line => (line.split("\t")(0), (if (line.split("\t").length > 1) {line.split("\t")(1).split(",").toList} else {List()})))
  val userDetail = sc.textFile(args(1))
  val userDob = userDetail.map(x => (x.split(",")(0), x.split(",")(9)))
  val userDobMap = userDob.collectAsMap()
  
  def callFunc(x: String, y: List[String]) : Double = {
    var avgAge = 0
    for (userID <- y){
      val dob = userDobMap.get(userID).toString().replace(")", "")
      val age = 2016 - (dob.split("/"))(2).toInt
      avgAge = avgAge + age
    }
    return avgAge.toDouble/y.length;
  }
  val t = userIdList.filter(x=> !x._2.isEmpty).map(x =>(x._1, callFunc(x._1, x._2)) )
  val userAgeMap = t.sortBy(_._2).collect().reverse.take(20)
  val userAgeMapRDD = sc.parallelize(userAgeMap).map(x=> (x._1, x._2))
  val userAddressMapRDD = userDetail.map(x => (x.split(",")(0), ( x.split(",")(3)+","+x.split(",")(4)+","+x.split(",")(5) ) ))
  userAddressMapRDD.join(userAgeMapRDD).foreach( t => println("%s %s".format(t._1, t._2)))  
  
  userAddressMapRDD.join(userAgeMapRDD).saveAsTextFile(args(2))
  }
}
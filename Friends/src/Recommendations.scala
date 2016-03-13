import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Recommendations {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Recommendations").setMaster("local");
    val sc = new SparkContext(conf)
    val userIdListFile = sc.textFile(args(0))
    val userIdAndList = userIdListFile.map(line => (line.split("\t")(0), (if (line.split("\t").length > 1) {line.split("\t")(1).split(",").toList} else {List()})))
    var userIdListMap = userIdAndList.collectAsMap()
    
    def callFunc(x : String , y:  List[String]) : List[String] = {
      var resList = List[String]()
        for(frId <- y){
              val l = userIdListMap.getOrElse(frId, List())
              if(l.length > 0){
                 resList = l.diff(y).diff(List(x)) ::: resList
              }
        }
      return resList
    }
    val frListMapRDD = userIdAndList.filter(x=> !x._2.isEmpty).map(t=>(t._1, callFunc(t._1, t._2)) )
    //val friendListTop20 = sc.parallelize(frListMapRDD.sortBy(_._2.length).collect.reverse).take(20)
    frListMapRDD.foreach(y => println("%s  [%s]".format(y._1, y._2 mkString ",") ))
    
    frListMapRDD.saveAsTextFile(args(1))
  }
}
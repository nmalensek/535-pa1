import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Hits {
  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Incorrect number of arguments. Usage:")
      println("<search keyword> <filepath of article titles> <filepath of article links> <output directory> <local | yarn>")
      return
    }
    val searchTerm = args(0)
    val titlesFilepath = args(1)
    val linksFilepath = args(2)
    val outputPath = args(3)
    val specifiedMaster = args(4)
   
    val spark = SparkSession.builder.appName("TeamDenverHITS").master(specifiedMaster).getOrCreate() 

    val rootSet = getRootSet(titlesFilepath, searchTerm, spark.sparkContext)
    
    //shouldn't need to do this because we save the file immediately, but potentially we could
    //rootSet = rootSet.sortByKey() 
    rootSet.saveAsTextFile(outputPath + "rootSet/")
    
    //if we need the root set on one machine for grading purposes, use the following line instead of the one above.
    //rootSet.coalesce(1).saveAsTextFile(outputPath + "rootSet/")

    rootSet.cache()

    //val baseSet = getBaseSet(linksFilepath, searchTerm, spark.sparkContext) //?
  
  }

  def getRootSet(filePath: String, word: String, sc: SparkContext) : RDD[(Long,String)] = {
    sc.textFile(filePath).zipWithIndex()
      .filter(x => x._1.toLowerCase.split("_").contains(word.toLowerCase))
      .map(x => (x._2 + 1, x._1)) 
  }
}

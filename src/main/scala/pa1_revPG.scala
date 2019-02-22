import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
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

    //get all of the titles (need them later for calculating base set) and the root set
    val (allTitles, rootSet) = getRootSet(titlesFilepath, searchTerm, spark.sparkContext)
    
    rootSet.coalesce(1).saveAsTextFile(outputPath + "rootSet/")

    rootSet.cache()

    val allLinks = getAllLinksAsTuples(linksFilepath, spark.sparkContext)

    //get the root nodes' outgoing links and their incoming links
    val fromRootToOther = allLinks.join(rootSet).map(x => (x._1, x._2._1))
    val fromOtherToRoot = allLinks.map(x => (x._2, x._1)).join(rootSet).map(x => (x._2._1, x._1))

    //get the base set links and pages.
    val baseSetLinks = fromRootToOther.union(fromOtherToRoot).distinct()
    baseSetLinks.cache()

    //get the base set pages
    val baseSetPages = fromRootToOther.map(x => (x._2,x._1)).union(fromOtherToRoot).reduceByKey((x, y) => x)
                      .join(allTitles).map(x => (x._1, x._2._2)).union(rootSet).reduceByKey((x, y) => x)   //why not distinct here? which is better?
    baseSetPages.cache()
    baseSetPages.coalesce(1).saveAsTextFile(outputPath + "baseSet/")



    //*********************** - Prerana

    //calculating authority scores
    var authScores = baseSetLinks.map(x => (x._1, 1.0)).union(baseSetLinks.map(x => (x._2, 1.0))).distinct()
    var hubScores = authScores

    for (i <- 1 to 5) {//hardcoded iters for now
      val absAuthScores = baseSetLinks.join(hubScores).values.reduceByKey(_ + _)
      val sumVal = absAuthScores.values.sum()
      authScores = absAuthScores.mapValues(x => x/sumVal)

      val absHubScores = baseSetLinks.map(x => (x._2, x._1)).join(authScores).values.reduceByKey(_ + _)
      val sumVal2 = absHubScores.values.sum()
      hubScores = absHubScores.mapValues(x => x/sumVal2)
    }  

    val authTitles = authScores.join(baseSetPages).sortByKey().map(x => (x._2._2, x._2._1))
    authTitles.cache()
    authTitles.coalesce(1).saveAsTextFile(outputPath + "authTitles/")

    val hubTitles = hubScores.join(baseSetPages).sortByKey().map(x => (x._2._2, x._2._1))
    hubTitles.cache()
    hubTitles.coalesce(1).saveAsTextFile(outputPath + "hubTitles/")    
    
    //************************


  def getRootSet(filePath: String, word: String, sc: SparkContext) : (RDD[(Long,String)], RDD[(Long, String)]) = {
    val allTitlesWithIndex = sc.textFile(filePath).zipWithIndex().map(x => (x._2 + 1, x._1))
    val rootSet = allTitlesWithIndex.filter(x => x._2.toLowerCase.split("_").contains(word.toLowerCase))
    (allTitlesWithIndex, rootSet)
  }

  def getAllLinksAsTuples(filePath: String, sc: SparkContext) : RDD[(Long, Long)] = {
    //get tuples of links from one site to another in the form of (source, destination)
    sc.textFile(filePath).map(x => (x.split(": ")(0).toLong, x.split(": ")(1).split(" ").map(i => i.toLong)))
    .flatMapValues(x => x)
  }
}

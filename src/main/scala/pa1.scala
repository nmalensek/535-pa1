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
                      .join(allTitles).map(x => (x._1, x._2._2)).union(rootSet).reduceByKey((x, y) => x)
    baseSetPages.cache()
    baseSetPages.coalesce(1).saveAsTextFile(outputPath + "baseSet/")

    //****
    // dunno if anything works past this point
    //****
    var authScores = baseSetPages.map(x => (x._1, 1.0))
    var hubScores = authScores
  
    while(true) {
    //calculate authority scores
    var authStep1 = baseSetLinks.join(hubScores).map(x => (x._2._1, x._2._2)).reduceByKey((x, y) => x+y)
    var totalHub = authStep1.values.sum()
    authScores = authStep1.map(x => (x._1, (x._2/totalHub))).rightOuterJoin(hubScores).map(x => (x._1,x._2._1.getOrElse(0.0)))
    
    //calculate hub scores
    }

/**
    var step1 = baseSetLinks.join(hubScores).map(x => (x._2._1, x._2._2))
    //Array[(Long, Double)] = Array((1,1.0), (8,1.0), (2,1.0), (7,1.0), (2,1.0), (10,1.0), (5,1.0), (1,1.0), (2,1.0))

    var step2 = step1.reduceByKey((x, y) => x+y)
    //Array[(Long, Double)] = Array((1,2.0), (7,1.0), (8,1.0), (2,3.0), (10,1.0), (5,1.0))

    var totalHub = step2.values.sum()
    //9.0

    var step3 = step2.map(x => (x._1, (x._2/totalHub)))
    //Array[(Long, Double)] = Array((1,0.2222222222222222), (7,0.1111111111111111), (8,0.1111111111111111), (2,0.3333333333333333), (10,0.1111111111111111), (5,0.1111111111111111))
    
    var authScores = step3.rightOuterJoin(hubScores).map(x => (x._1,x._2._1.getOrElse(0.0)))
    //Array[(Long, Double)] = Array((6,0.0), (1,0.2222222222222222), (7,0.1111111111111111), (8,0.1111111111111111), (2,0.3333333333333333), (4,0.0), (10,0.1111111111111111), (5,0.1111111111111111))

    var hStep1 = baseSetLinks.map(x => (x._2,x._1)).join(authScores).map(x => (x._2._1,x._2._2))
    //Array[(Long, Double)] = Array((1,0.1111111111111111), (1,0.1111111111111111), (1,0.3333333333333333), (2,0.1111111111111111), (2,0.1111111111111111), (4,0.2222222222222222), (5,0.3333333333333333), (6,0.2222222222222222), (8,0.3333333333333333))


    */
  }

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

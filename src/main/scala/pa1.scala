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
    var authScores = baseSetLinks.map(x => (x._1, 1.0))
    var hubScores = authScores
  
    while(true) {
      //calculate hub scores
      //calculate authority scores
    }

/**
    val step1 = baseSetLinks.join(hubScores).map(x => (x._2._1, x._2._2))
    //Array[(Long, Double)] = Array((2,1.0), (5,1.0), (10,1.0), (6,1.0), (8,1.0), (2,1.0), (2,1.0))

    val step2 = step1.reduceByKey((x, y) => x+y)
    //Array[(Long, Double)] = Array((2,3.0), (5,1.0), (6,1.0), (8,1.0), (10,1.0))

    val step3 = step2.rightOuterJoin(hubScores).map(x => (x._1,x._2._1.getOrElse(0)))
    //Array[(Long, AnyVal)] = Array((1,0), (2,3.0), (5,1.0), (6,1.0), (8,1.0), (10,1.0))
    //take these and divide each of them by the normalization value (7 in this example case)
    
    var normalize = step2.values.sum()
    //7.0

    //next step map back to hub score w/ division
    hubScores = step3.
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

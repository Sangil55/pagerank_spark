package pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


class pagerank {

    def printt(rdd : Any): Unit = rdd match {
      case rd : Array[(Any,Array[String],Double)]=> {
        for(i <-rd)
        {
          var (key : Any,value : Array[String] ,value2 : Double) = i
          print(key)
          for(j<-value)
            print( " " + j )
          print (" >>> " + value2)
          println()
        }
      }

      case rd : Array[(Any,Array[String])]=> {
        var rdprints = rd
        for(i <-rdprints)
        {
          var (key : Any,value : Array[String]) = i
          print(key)
          for(j<-value)
            print( " " + j )
          println()
        }
      }
      case rd : Array[(Any,Any)]=> {
        var rdprints = rd
        for(i <-rdprints)
        {
          var (key : Any,value : Any) = i
          print(key)
          //for(j<-value)
            print( " " + value )
          println()
        }
      }

      case _ =>
        println("TT")

    }

  class CustomPartiotioner(numParts : Int) extends Partitioner {
    override def numPartitions: Int = numParts
    override def getPartition(key: Any): Int =
    {
       key.toString.toInt-1
    }

    override def equals(obj: scala.Any): Boolean = super.equals(obj)
  }


    def job(mode : String) : Unit = {
    val t0 = System.nanoTime()

    //val conf = new SparkConf().setMaster("yarn").setAppName("My App")

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder.appName("pagerank").master("yarn").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    var jsfile = spark.read.json("/user/hadoop/wiki_links")
    jsfile.show()
    val ds = jsfile.select("title","new_Links")
      .map(row=>(row.get(0).toString,row.getSeq[String](1)))
      .withColumnRenamed("_1","title").withColumnRenamed("_2","links")
    ds.show()
    //rdds.map{ case(title,links)=>(line.split(" ")(0),(line.split(" ")) ))
      ds.unpersist();
    //ds.repartition(10).persist()
    var ranks = ds.map(row=>(row.get(0).toString,1.0)).withColumnRenamed("_1","title")
    val itr = mode.toInt
    for(i<-0 until itr) {
      println("itteration " + i)
      val check = ds.join(ranks, "title")
      // check.show()
      val contributions = check.flatMap {
        case row => {
          var links: Seq[String] = row.getSeq[String](1) :+ row.get(0).toString
          val rank: Double = row.getDouble(2)
          links.map(dest => (dest, rank / links.size))
        }
      }
      //  contributions.show()
      val temps = contributions.map(r => (r._1, r._2)).groupByKey(_._1).reduceGroups((a, b) => (a._1, a._2 + b._2))
      ranks = temps.map(r => (r._1, 0.15 + 0.85 * r._2._2)).withColumnRenamed("_1","title")
      //  ranks.show()
    }
    ds.join(ranks, "title").show()

    ds.join(ranks, "title").write.text("/home/hadoop/pagerankresult.txt")

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    //ranks.saveAsTextFile("ranks")*/
  }
}

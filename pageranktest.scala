package pagerank

//package pagerank

object pageranktest {
  def main(args: Array[String]): Unit = {
    //   val ac = new akka2()
    //   ac.job();

    var a = ""
    if(args.length>0)
      a = args(0)
    val f = new pagerank()
    f.job(a)
  }
}

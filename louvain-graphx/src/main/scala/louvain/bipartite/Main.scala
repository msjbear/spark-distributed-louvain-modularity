package louvain.bipartite

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("SparkWcAna-wclouvain-dev")
    val sc = new SparkContext(conf)

    val edgeFile = "data/louvain/edges_sample.txt"
    val outputdir = "data/louvain/test_result"
    val sep = "\t"
    val minProgress=1
    val progressCounter=1
    
    val edgeRDD = sc.textFile(edgeFile).map(row => {
      val tokens = row.split(sep).map(_.trim())
      tokens.length match {
        case 2 => {
          new Edge(tokens(0).toLong, tokens(1).toLong, 1.0D)
        }
        case 3 => {
          new Edge(tokens(0).toLong, tokens(1).toLong, tokens(2).toDouble)
        }
        case 5 => {
          new Edge(tokens(0).toLong, tokens(1).toLong, tokens(4).toDouble)
        }
        case _ => {
          throw new IllegalArgumentException("invalid input line: " + row)
        }
      }
    })
    
    val graph = Graph.fromEdges(edgeRDD, None)
    
    val runner = new LouvainHarness(minProgress, progressCounter, outputdir)
    runner.run(sc, graph)
    
  }
}
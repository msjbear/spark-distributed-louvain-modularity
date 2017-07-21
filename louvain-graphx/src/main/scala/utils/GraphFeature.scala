package utils

import org.apache.spark.graphx.{VertexRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.utils.Logging

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by meishangjian on 2017/5/8.
  */
object GraphFeature extends Logging {

  def getGraphWithDegree[ED: ClassTag](g: Graph[Array[Any], ED]): Graph[Array[Any], ED] = {
    val degree = g.degrees

    val graph = g.outerJoinVertices(degree) { (id, a, o) =>
      val ab = ArrayBuffer[Any]()
      ab ++= a
      ab += o.getOrElse(DefaultValueUtils.intCountMissingVal)
      ab.toArray
    }

    graph
  }

  def getGraphWithPageRank[ED: ClassTag](g: Graph[Array[Any], ED], ranks: VertexRDD[Double]): Graph[Array[Any], ED] = {
    val graph = g.outerJoinVertices(ranks) { (id, a, o) =>
      val ab = ArrayBuffer[Any]()
      ab ++= a
      ab += o.getOrElse(DefaultValueUtils.intCountMissingVal)
      ab.toArray
    }

    graph
  }

  //cc subgraph num, vertices num in each subgraph
  def getCCVertices[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]): VertexRDD[VertexId] = {
    val connectedComponent = g.connectedComponents()
    val ccv = connectedComponent.vertices
    ccv
  }

  /**
   * ccid
    *
    * @param g: graph
   * @param ccv: cc vertices
   * @return
   */
  def getGraphWithCCId[VD: ClassTag, ED: ClassTag](g: Graph[Array[Any], ED], ccv: VertexRDD[VertexId]): Graph[Array[Any], ED] = {
    val graph: Graph[Array[Any], ED] = g.outerJoinVertices(ccv) { (id, a, o) =>
      val ab = ArrayBuffer[Any]()
      ab ++= a
      ab += o.getOrElse(DefaultValueUtils.vidMissingVal)
      ab.toArray
    }

    graph
  }

  /**
   * vertices num of each cc subgraph
   */
  def getGraphWithCCVerticsNum[VD: ClassTag, ED: ClassTag](g: Graph[Array[Any], ED], cc: Graph[VertexId, ED]): Graph[Array[Any], ED] = {
    val numMap = cc.vertices.map {
      e => (e._2, 1)
    }.reduceByKey(_ + _).collectAsMap()

    val cv = cc.mapVertices {
      (vid, cid) => (numMap.getOrElse(cid, 0))
    }.vertices

    val graph: Graph[Array[Any], ED] = g.outerJoinVertices(cv) {
      (id, a, o) =>
        val ab = ArrayBuffer[Any]()
        ab ++= a
        ab += o.getOrElse(DefaultValueUtils.countMissingVal)
        ab.toArray
    }

    graph
  }

  /**
   * get the ids of connected component in the graph, the id is the smallest vid in the cc
    *
    * @param ccv:ccVertices
   * @tparam VD
   * @tparam ED
   * @return
   */
  def getCCId[VD: ClassTag, ED: ClassTag](ccv: VertexRDD[VertexId]): RDD[VertexId] = {
    val ccId = ccv.map { vi =>
      vi._2
    }

    ccId
  }

  //neighbours num
  def getNbrJumpMap[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED]) = {
    type VMap = Map[VertexId, Int]

    /** * merge message */
    def addMaps(spmap1: VMap, spmap2: VMap): VMap = (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

    val gMessage0: VertexRDD[Map[VertexId, Int]] = graph.aggregateMessages(
      tpt => {
        // Map Function
        tpt.sendToDst(Map[VertexId, Int](tpt.srcId -> 1) ++ Map[VertexId, Int](tpt.dstId -> 0))
        tpt.sendToSrc(Map[VertexId, Int](tpt.dstId -> 1) ++ Map[VertexId, Int](tpt.srcId -> 0))
      },
      addMaps
    )
    val gM1 = graph.outerJoinVertices(gMessage0)((vid, attr, m) => m.getOrElse(Map[VertexId, Int](vid -> 0)))
    //    gM1.vertices.foreach(println)

    val gMessage1: VertexRDD[Map[VertexId, Int]] = gM1.aggregateMessages(
      tpt => {
        // Map Function
        tpt.sendToDst((tpt.srcAttr.keySet).map(k => k -> (tpt.srcAttr(k) + 1)).toMap ++ tpt.dstAttr)
        tpt.sendToSrc((tpt.dstAttr.keySet).map(k => k -> (tpt.dstAttr(k) + 1)).toMap ++ tpt.srcAttr)
      },
      // Reduce Function
      addMaps
    )

    val gM2 = graph.outerJoinVertices(gMessage1)((vid, attr, m) => m.getOrElse(Map[VertexId, Int](vid -> 0)))
    gM2
  }

  def getTwoJumpNbrWithPregel[VD: ClassTag, ED: ClassTag, VD2: ClassTag](g: Graph[VD, ED]): VertexRDD[Iterable[VertexId]] = {
    //pregel
    type VMap = Map[VertexId, Int]

    /** * updata node data, also named colluction union */
    def vprog(vid: VertexId, vdata: VMap, message: VMap): Map[VertexId, Int] = addMaps(vdata, message)

    /** * send message */
    def sendMsg(e: EdgeTriplet[VMap, _]) = {
      //diff of two collection, and value minus 1
      val srcMap = (e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k -> (e.dstAttr(k) - 1) }.toMap
      val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k -> (e.srcAttr(k) - 1) }.toMap

      if (srcMap.size == 0 && dstMap.size == 0) Iterator.empty else Iterator((e.dstId, dstMap), (e.srcId, srcMap))
    }

    /** * merge message */
    def addMaps(spmap1: VMap, spmap2: VMap): VMap = (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

    val two = 2 //two jump neighbor
    val newG = g.mapVertices((vid, _) => Map[VertexId, Int](vid -> two))
        .pregel(Map[VertexId, Int](), two, EdgeDirection.Out)(vprog, sendMsg, addMaps)

    //two jump, value=0
    val twoJumpNbrs = newG.vertices.mapValues(_.filter(_._2 == 0).keys)

    twoJumpNbrs //VertexRDD[Iterable[VertexId]]
  }

  def getGraphWithTwoJumpNum[ED: ClassTag](g: Graph[Array[Any], ED]): Graph[Array[Any], ED] = {

    val tjs = getTwoJumpNbrWithPregel(g)
    val tjNum = tjs.mapValues {
      (id, c) => c.size
    }

    val graph = g.outerJoinVertices(tjNum) { (id, a, o) =>
      val ab = ArrayBuffer[Any]()
      ab ++= a
      ab += o.getOrElse(DefaultValueUtils.countMissingVal)
      ab.toArray
    }

    graph
  }

  def main(args: Array[String]) {
    val vertexPath = "data/louvain/edges_sample.txt"
    val edgePath = "data/louvain/vertex_sample.txt"

    val spark = SparkSession
      .builder
      .master("local")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    val vparam = Array(0, 1, 3)
    val eparam = Array(0, 1) //graphEdgeData.csv

    val vertexData = DataLoadUtils.loadBiPartiteVertexFromOrcFile(spark, vertexPath, vparam(0), vparam(1), vparam(2))
    val edgeData = DataLoadUtils.loadBiPartiteEdgeFromOrcFile(spark, edgePath, eparam(0), eparam(1))

    var g = Graph(vertexData, edgeData)

    val ccv = getCCVertices(g)
    logWarning("JRDM:"+"cc vertices num: " + ccv.count())

    val ccId = getCCId(ccv)
    logWarning("JRDM:"+"ccIds num: " + ccId.distinct().count)

    g = getGraphWithCCId(g, ccv)

    val cc = g.connectedComponents()
    g = getGraphWithCCVerticsNum(g, cc)

    g = getGraphWithDegree(g)

    g = getGraphWithTwoJumpNum(g)

    val ranks = g.pageRank(0.0001).vertices
    g = getGraphWithPageRank(g, ranks)

    val countV = g.vertices.count()
    logWarning("JRDM:"+"getGraphWithCCId, vertices num:" + countV)
    g.triplets.collect.map {
      e => e.srcId + ",(" + e.srcAttr.mkString(",") + ")," + e.dstId + ",(" + e.dstAttr.mkString(",") + ")," + e.attr.toString
    }.foreach(println)

  }

}

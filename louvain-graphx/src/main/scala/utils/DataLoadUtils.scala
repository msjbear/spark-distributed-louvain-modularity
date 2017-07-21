package utils

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.utils.Logging

import scala.collection.mutable.ArrayBuffer

/**
  * Created by meishangjian on 2017/5/8.
  */
object DataLoadUtils extends Logging {

  /**
    * bipartite graph vertex
    * @param spark
    * @param filePath
    * @param vid
    * @param begin
    * @param end
    * @param sep
    * @param numPartitions
    * @return
    */
  def loadBiPartiteVertexFromOrcFile(spark: SparkSession, filePath: String, vid: Int, begin: Int, end: Int, sep: String ="\t", numPartitions:Int=100): RDD[(VertexId, Array[Any])] = {
    val data = spark.read.text(filePath).distinct.repartition(numPartitions)
    var vertextData: RDD[(VertexId, Array[Any])] = data.rdd.map { row =>
      var attrs = new ArrayBuffer[Any]
      val parts = row.mkString(sep).split(sep,-1)
      var vertexid = DefaultValueUtils.vidLongMissingVal
      try {
        vertexid = parts(vid).toLong
        for (i <- begin to end) {
          attrs += parts(i)
        }
      } catch {
        case _: Exception => logError("JRDM:"+"parts length: " + row.length + ", vertexId value:" + row(vid))
      }

      if (vertexid < 0) {
        (vertexid, Array.empty: Array[Any])
      } else {
        (vertexid, attrs.toArray)
      }
    }
    vertextData = vertextData.filter(e => e._1 != DefaultValueUtils.vidLongMissingVal)

    vertextData
  }

  /**
    * bilouvain edge
    * @param spark
    * @param filePath
    * @param sid
    * @param did
    * @param sep
    * @param numPartitions
    * @return
    */
  def loadBiPartiteEdgeFromOrcFile(spark: SparkSession, filePath: String, sid: Int, did: Int, sep: String ="\t", numPartitions:Int=100): RDD[Edge[EdgeAttr]] = {
    val data = spark.read.text(filePath).distinct.repartition(numPartitions)

    val edgeData: RDD[Edge[EdgeAttr]] = data.rdd.map { row =>
      val parts = row.mkString(sep).split(sep,-1)
      var srcId = DefaultValueUtils.vidLongMissingVal
      var dstId = DefaultValueUtils.vidLongMissingVal
      val edgeAttr = new EdgeAttr
      var edge: Edge[EdgeAttr] = null

      try {
        srcId = DefaultValueUtils.getVidLongVal(parts(sid))
        dstId = DefaultValueUtils.getVidLongVal(parts(did))
        try {
          edgeAttr.relationVal = DefaultValueUtils.getStringVal(parts(2))
          edgeAttr.relationType = DefaultValueUtils.getStringVal(parts(3))
          edgeAttr.count = DefaultValueUtils.getWeightVal(parts(4))
        } catch {
          case _: Exception => logError("JRDM:" + "attr: parts length: " + row.length + ", line value:" + row)
        }
      } catch {
        case _: Exception => logError("JRDM:" + "sid, did: parts length: " + row.length + ", line value:" + row)
      }
      edge = Edge(srcId, dstId, edgeAttr)

      edge
    }

    edgeData.filter(e=>e.srcId!= DefaultValueUtils.vidLongMissingVal && e.dstId != DefaultValueUtils.vidLongMissingVal)
  }
}

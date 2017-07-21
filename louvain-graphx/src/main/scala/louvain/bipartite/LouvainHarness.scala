package louvain.bipartite

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import utils.LouvainUtils

import scala.reflect.ClassTag

class LouvainHarness(minProgress: Int, progressCounter: Int, outputdir: String) {

  
  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Double]) = {

    var louvainGraph = LouvainCore.createLouvainGraph(graph)
    var rawGraph = louvainGraph
    var level = -1 // number of times the graph has been compressed
    val q = -1.0 // current modularity value
    var halt = false

    var vertexRaw = louvainGraph.vertices.mapValues((vid, state) => (state.community, (state.communitySigmaTot, vid))).values
    do {
      level += 1
      
      val (currentQ, currentGraph) = LouvainCore.louvain(sc, louvainGraph, minProgress, progressCounter, level)

      val vertexUpdated = currentGraph.vertices.mapValues((vid: Long, state) => (vid, (state.community, state.communitySigmaTot))).values
      vertexRaw = LouvainCore.updateVertexCommunity(vertexRaw, vertexUpdated)

      vertexUpdated.unpersist(blocking = false)
      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph
      
      println("---- level: "+level+"\t currentQ: "+currentQ)
    
      saveLevel(sc, level, currentQ, louvainGraph)
      
      if (level==0){
        louvainGraph = LouvainCore.compressGraph(louvainGraph)
      }else{
        halt= true
      }
    } while (!halt)
    rawGraph = LouvainCore.updateGraphCommunity(rawGraph, vertexRaw)
    rawGraph.vertices.mapValues((vid,vstate)=>vstate.toString()).foreach(println)
    finalSave(sc, level, q, rawGraph)
  }

  
  var qValues = Array[(Int, Double)]()
  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Double]) = {
    if(level==0){
      LouvainUtils.deleteFile(outputdir+"layer_first")
      graph.vertices.map( {case (id,v) => id+"\t"+v.community}).saveAsTextFile(outputdir+"layer_first")
    }else{
      LouvainUtils.deleteFile(outputdir+"layer_second")
      graph.vertices.map( {case (id,v) => id+"\t"+v.community}).saveAsTextFile(outputdir+"layer_second")
    }
  }

  
  def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Double]) = {
    val resultPath = outputdir+"_final_result"
    LouvainUtils.deleteFile(resultPath)
    graph.vertices.mapValues((vid,vstate)=>vstate.toString()).saveAsTextFile(resultPath)

  }


}
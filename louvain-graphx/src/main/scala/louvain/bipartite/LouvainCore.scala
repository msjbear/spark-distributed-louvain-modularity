package louvain.bipartite

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object LouvainCore {


  def createLouvainGraph[VD: ClassTag](graph: Graph[VD, Double]): Graph[VertexState, Double] = {
    // Create the initial Louvain graph.  
    // 初始化根据边构件图的点信息
    val nodeWeightMapFunc = (e: EdgeContext[VD, Double, Double]) => {
      e.sendToDst(e.attr)
      e.sendToSrc(e.attr)
    }
    val nodeWeightReduceFunc = (e1: Double, e2: Double) => e1 + e2
    val nodeWeights = graph.aggregateMessages(nodeWeightMapFunc, nodeWeightReduceFunc)

    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0.0D)
      val state = new VertexState()
      state.community = vid             //归属的社区ID(初始化归属于自己社区)
      state.communitySigmaTot = weight  //归属社区的度(初始化是点的度)
      state.internalWeight = 0.0D       //源社区的边 = 内部边数*2+外部边数(初始化是零)
      state.nodeWeight = weight         //源社区的度 = 外部边数(初始化是点的度)
      state.changed = false
      state.vtype = get_vtype(vid)
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)
    // printlouvain(louvainGraph)
    // printedgetriplets(louvainGraph)
    return louvainGraph

  }


  def get_vtype(vid: Long):Int = {
    if (vid<=10000000000L) 0 //区分点的类型，用户id小于100亿
    else 1
  }


  /**
    * Transform a graph from [VD,Long] to a a [VertexState,Long] graph and label each vertex with a community
    * to maximize global modularity (without compressing the graph)
    */
  def louvainFromStandardGraph[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Double], minProgress: Int = 1, progressCounter: Int = 1, level: Int): (Double, Graph[VertexState, Double]) = {
    val louvainGraph = createLouvainGraph(graph)
    return louvain(sc, louvainGraph, minProgress, progressCounter, level)
  }


  /**
    * For a graph of type Graph[VertexState,Long] label each vertex with a community to maximize global modularity.
    * (without compressing the graph)
    */
  def louvain(sc: SparkContext, graph: Graph[VertexState, Double], minProgress: Int = 1, progressCounter: Int = 1,
              level: Int): (Double, Graph[VertexState, Double]) = {
    var louvainGraph = graph.cache()

    val graphWeight = louvainGraph.vertices.values.map(vdata => vdata.internalWeight + vdata.nodeWeight).reduce(_ + _)
    val totalGraphWeight = sc.broadcast(graphWeight)
    println("\n---- graphWeight: "+graphWeight)
    
    // gather community information from each vertex's local neighborhood
    var msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg)
    var activeMessages = msgRDD.count() //materializes the msgRDD and caches it in memory

    var updated = 0L - minProgress
    var even = false
    var count = 0
    val maxIter = if (level==0) 12 else 16
//    val maxIter = 6
    var stop = 0
    var updatedLastPhase = 0L
    do {
      count += 1
      even = !even
    
      // label each vertex with its best community based on neighboring community information
      val labeledVerts = louvainVertJoin(louvainGraph, msgRDD, totalGraphWeight, even, level).cache()
      
      // calculate new sigma total value for each community (total weight of each community)
      // 计算合并社区后新社区的内部节点的度之和sigmaTot
      val communtiyUpdate = labeledVerts
        .map({ case (vid, vdata) => (vdata.community, vdata.nodeWeight + vdata.internalWeight) })
        .reduceByKey(_ + _).cache()
        
      val communtiynum = communtiyUpdate.count()

      // map each vertex ID to its updated community information
      // 新建RDD存储节点及更新节点归属的社区信息（新社区ID和新社区内部节点度之和
      val communityMapping = labeledVerts
        .map({ case (vid, vdata) => (vdata.community, vid) })
        .join(communtiyUpdate)
        .map({ case (community, (vid, sigmaTot)) => (vid, (community, sigmaTot)) })
        .cache()
      
      // join the community labeled vertices with the updated community info
      // 完成更新节点和社区的对应关系，社区ID和社区内部节点度之和
      val updatedVerts = labeledVerts.join(communityMapping).map({ case (vid, (vdata, communityTuple)) =>
        vdata.community = communityTuple._1
        vdata.communitySigmaTot = communityTuple._2
        (vid, vdata)
      }).cache()

      
      updatedVerts.count()
      labeledVerts.unpersist(blocking = false)
      communtiyUpdate.unpersist(blocking = false)
      communityMapping.unpersist(blocking = false)

      louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
      louvainGraph.cache()
      
      // gather community information from each vertex's local neighborhood
      val oldMsgs = msgRDD
      msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg).cache()
      activeMessages = msgRDD.count() // materializes the graph by forcing computation
      
      oldMsgs.unpersist(blocking = false)
      updatedVerts.unpersist(blocking = false)

      // half of the communites can swtich on even cycles
      // and the other half on odd cycles (to prevent deadlocks)
      // so we only want to look for progess on odd cycles (after all vertcies have had a chance to move)
      if (even) updated = 0
      updated = updated + louvainGraph.vertices.filter(_._2.changed).count
      if (!even) {
        //println("  # vertices moved: " + java.text.NumberFormat.getInstance().format(updated))
        if (updated >= updatedLastPhase - minProgress) stop += 1
        updatedLastPhase = updated
      }
      println("-------- Count:  " + count + " Updated: " + updated + " communtiynum:" +communtiynum)

    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIter)))
    // 奇数次不会停
    // 偶数次循环才判断这一次updated的数量是不是比上一次偶数次循环大minProgress，如果大于就增加stop的值（满足变化最小条件的迭代次数），stop值大于progressCounter(1)就停止
    // 上述情况说明已经收敛了，迭代的点都找到了最应该归属的社区，出现前后两次相等或者变大，说明有对称结构，社区来回变化
    // 偶数循环，就算progressCounter，只要updated梳理不大于零或者count超过了maxiter也会停止
    
    println("---- Completed in " + count + " cycles")
      
    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      // sum the nodes internal weight and all of its edges that are in its community
      val community = vdata.community
      var k_i_in = vdata.internalWeight
      val sigmaTot = vdata.communitySigmaTot.toDouble
      msgs.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        if (vdata.community == communityId) k_i_in += communityEdgeWeight
      })
      val M = totalGraphWeight.value
      val k_i = vdata.nodeWeight + vdata.internalWeight
      val q = (k_i_in.toDouble / M) - ((sigmaTot * k_i) / math.pow(M, 2))
//      println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
      if (q < 0) 0 else q
    })
    val actualQ = newVerts.values.reduce(_ + _)

    return (actualQ, louvainGraph)
  }


  /**
    * Creates the messages passed between each vertex to convey neighborhood community data.
    */
  private def sendMsg(et: EdgeContext[VertexState, Double, Map[(Long, Double), Double]]) = {
    val m1 = Map((et.srcAttr.community, et.srcAttr.communitySigmaTot) -> et.attr)
    val m2 = Map((et.dstAttr.community, et.dstAttr.communitySigmaTot) -> et.attr)
    et.sendToDst(m1)
    et.sendToSrc(m2)
  }


  /**
    * Merge neighborhood community data into a single message for each vertex
    */
  private def mergeMsg(m1: Map[(Long, Double), Double], m2: Map[(Long, Double), Double]) = {
    val newMap = scala.collection.mutable.HashMap[(Long, Double), Double]()
    m1.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }


  /**
    * Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
    * Returns a new set of vertices with the updated vertex state.
    */
  private def louvainVertJoin(louvainGraph: Graph[VertexState, Double], msgRDD: VertexRDD[Map[(Long, Double), Double]],
                              totalEdgeWeight: Broadcast[Double], even: Boolean, level: Int) = {
    louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      var bestCommunity = vdata.community
      val startingCommunityId = bestCommunity
      var maxDeltaQ = BigDecimal(0.0)
      var bestSigmaTot = 0.0D
      
      msgs.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vdata.nodeWeight, vdata.internalWeight, totalEdgeWeight.value, level)
        //println(" "+startingCommunityId+"  "+communityId+"  "+sigmaTotal+"  "+communityEdgeWeight+"  "+vdata.nodeWeight+"  "+vdata.internalWeight+"  "+totalEdgeWeight.value+"  "+deltaQ)
        //相同bestcommunity时，社区ID更大才更新bestcommunity,即bestcommunity里面是最大的社区ID
        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))) {
          if(level == 0 && vdata.vtype == 0){
            maxDeltaQ = deltaQ
            bestCommunity = communityId
            bestSigmaTot = sigmaTotal
          } else if(level > 0 && deltaQ > 0.2){
            maxDeltaQ = deltaQ
            bestCommunity = communityId
            bestSigmaTot = sigmaTotal
          }

        }
//        println(" "+vid+" "+startingCommunityId+"  "+communityId+"  "+sigmaTotal+"  "+communityEdgeWeight+"  "+vdata.nodeWeight+"  "+vdata.internalWeight+"  "+totalEdgeWeight.value+"  "+deltaQ+" "+maxDeltaQ+"  "+bestCommunity+"  "+bestSigmaTot)
      })

      // only allow changes from low to high communties on even cyces and high to low on odd cycles
      // 奇数次循环，将社区ID变为更小的更好的ID。偶数次对换，将社区ID变为更大更好的ID，防止社区ID互变，1->5 5->1
      // 增加逻辑level级别为0，限制点向账号合并
      if (vdata.community != bestCommunity  && ((even && vdata.community > bestCommunity) || (!even && vdata.community < bestCommunity))) {
        if(level==0 && vdata.vtype==0){
          //println("  "+vid+" SWITCHED from "+vdata.community+" to "+bestCommunity)
          vdata.community = bestCommunity
          vdata.communitySigmaTot = bestSigmaTot
          vdata.changed = true
        }else if(level > 0){
          vdata.community = bestCommunity
          vdata.communitySigmaTot = bestSigmaTot
          vdata.changed = true
        }
      }
      else {
        vdata.changed = false
      }
      
      vdata
    })
  }


  /**
    * Returns the change in modularity that would result from a vertex moving to a specified community.
    */
  private def q(currCommunityId: Long, testCommunityId: Long, testSigmaTot: Double, edgeWeightInCommunity: Double, nodeWeight: Double, internalWeight: Double, totalEdgeWeight: Double, level: Int): BigDecimal = {
    val isCurrentCommunity = (currCommunityId.equals(testCommunityId));
    val M = BigDecimal(totalEdgeWeight);
    val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity;
    val k_i_in = BigDecimal(k_i_in_L);
    val k_i = BigDecimal(nodeWeight + internalWeight);
    val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot);

    var deltaQ = BigDecimal(0.0);
    if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
      if(level == 0){
        deltaQ = k_i_in + (k_i * sigma_tot / M)
      }else{
        deltaQ = k_i_in / k_i
      }
    }
    return deltaQ;
  }


  /**
    * Compress a graph by its communities, aggregate both internal node weights and edge
    * weights within communities.
    */
  def compressGraph(graph: Graph[VertexState, Double], debug: Boolean = true): Graph[VertexState, Double] = {

    // aggregate the edge weights of self loops. edges with both src and dst in the same community.
    // WARNING  can not use graph.mapReduceTriplets because we are mapping to new vertexIds

    // 结果是两列，第一列是社区ID，第二列是社区内部边数*2
    val internalEdgeWeights = graph.triplets.flatMap(et => {
      if (et.srcAttr.community == et.dstAttr.community) {
        Iterator((et.srcAttr.community, 2 * et.attr)) // count the weight from both nodes
      }
      else Iterator.empty
    }).reduceByKey(_ + _)
    
    // aggregate the internal weights of all nodes in each community
    val internalWeights = graph.vertices.values.map(vdata => (vdata.community, vdata.internalWeight)).reduceByKey(_ + _)

    // join internal weights and self edges to find new interal weight of each community
    val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({ case (vid, (weight1, weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0.0D)
      val state = new VertexState()
      state.community = vid
      state.communitySigmaTot = 0.0D
      state.internalWeight = weight1 + weight2
      state.nodeWeight = 0.0D
      state.changed = false
      (vid, state)
    }).cache()


    // translate each vertex edge to a community edge
    val edges = graph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) Iterator(new Edge(src, dst, et.attr))
//      if (src != dst) Iterator(new Edge(src, dst, et.attr))
      else Iterator.empty
    }).cache()


    // generate a new graph where each community of the previous
    // graph is now represented as a single vertex
    // 忽略权重合并
    val compressedGraph = Graph(newVerts, edges).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+ _)

    // calculate the weighted degree of each node
    val nodeWeightMapFunc = (e: EdgeContext[VertexState, Double, Double]) => {
      e.sendToDst(e.attr)
      e.sendToSrc(e.attr)
    }
    val nodeWeightReduceFunc = (e1: Double, e2: Double) => e1 + e2
    val nodeWeights = compressedGraph.aggregateMessages(nodeWeightMapFunc, nodeWeightReduceFunc)

    // fill in the weighted degree of each node
    // val louvainGraph = compressedGraph.joinVertices(nodeWeights)((vid,data,weight)=> {
    val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0.0D)
      data.communitySigmaTot = weight + data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph
    
    newVerts.unpersist(blocking = false)
    edges.unpersist(blocking = false)
    return louvainGraph


  }

  // debug printing
  private def printlouvain(graph: Graph[VertexState, Long]) = {
    print("\ncommunity label snapshot\n(vid,community,sigmaTot,internalWeight,nodeWeight,)\n")
    graph.vertices.mapValues((vid, vdata) => (vdata.community, vdata.communitySigmaTot, vdata.internalWeight, vdata.nodeWeight)).collect().foreach(f => println(" " + f))
  }


  // debug printing
  private def printedgetriplets(graph: Graph[VertexState, Long]) = {
    print("\ncommunity label snapshot FROM TRIPLETS\n(vid,community,sigmaTot)\n")
    (graph.triplets.flatMap(e => Iterator((e.srcId, e.srcAttr.community, e.srcAttr.communitySigmaTot), (e.dstId, e.dstAttr.community, e.dstAttr.communitySigmaTot))).collect()).foreach(f => println(" " + f))
  }

  def updateVertexCommunity(vertexRaw: RDD[(Long, (Double, VertexId))],
                            vertexUpdated: RDD[(Long, (Long, Double))]): RDD[(Long, (Double, VertexId))] = {
    val vertexNew = vertexRaw.leftOuterJoin(vertexUpdated)
      .map {
        case (community, ((communitySigmaTot, vid), Some(newCommunity))) =>
          (newCommunity._1, (newCommunity._2, vid))
        case (community, ((communitySigmaTot, vid), None)) =>
          (community, (communitySigmaTot, vid))
      }.cache()
    vertexRaw.unpersist(blocking = false)
    vertexUpdated.unpersist(blocking = false)
    vertexNew
  }

  def updateGraphCommunity(rawGraph: Graph[VertexState, Double], vertexNew: RDD[(Long, (Double, VertexId))])
  : Graph[VertexState, Double] = {
    val vertexFinal = vertexNew.map(r => (r._2._2, (r._1, r._2._1)))
    val graph = rawGraph.outerJoinVertices(vertexFinal) { (vid, oldVertexState, newVertexState) => {
      val newVertex = newVertexState.getOrElse((oldVertexState.community, oldVertexState.communitySigmaTot))
      val state = new VertexState
      state.community = newVertex._1
      state.communitySigmaTot = newVertex._2
      state.internalWeight = oldVertexState.internalWeight
      state.nodeWeight = oldVertexState.nodeWeight
      state
    }
    }.cache()
    rawGraph.unpersistVertices(blocking = false)
    vertexNew.unpersist(blocking = false)
    graph
  }


}
package louvain.bipartite

class VertexState extends Serializable {

  var community = -1L
  var communitySigmaTot = 0.0D
  var internalWeight = 0.0D
  // self edges
  var nodeWeight = 0.0D
  var changed = false
  var vtype = 0L

  override def toString(): String = {
    "{community:" + community + ",communitySigmaTot:" + communitySigmaTot +
      ",internalWeight:" + internalWeight + ",nodeWeight:" + nodeWeight +""+ ",vtype:"+ vtype + "}"
  }
}
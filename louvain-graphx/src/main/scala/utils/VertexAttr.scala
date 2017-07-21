package utils

/**
  * Created by meishangjian on 2017/5/8.
  */
class VertexAttr() extends Serializable {
  var pin: String = ""
  var riskVal: Double = 0D
  // 节点度值
  var degree: Long = 0L
  var tcNum: Int = 0
  var pageRank: Double = 0D
  var ccId: Long = 0L
  var ccVerticesNum: Int = 0
  // 顶点所属社区
  var community: Long = 0L
  var commVertexNum: Int = 0
  // 顶点所属社区的模块度
  var modularity: Double = 0D
  //下单日期；取风险值时，取下单日期最大的记录对应的风险值
  var orderDate: String = DefaultValueUtils.dateStrMissingVal

  override def toString(): String = {
    val sep = DefaultValueUtils.FIELD_SEP
      pin + sep +
      riskVal + sep +
      degree + sep +
      tcNum + sep +
      pageRank + sep +
      ccId + sep +
      ccVerticesNum + sep +
      community + sep +
      commVertexNum + sep +
      modularity + sep +
      orderDate
  }

  def toJson(): String = {
    "pin:" + pin +
      ", riskVal:" + riskVal +
      ", degree:" + degree +
      ", tcNum:" + tcNum +
      ", pageRank:" + pageRank +
      ", ccId:" + ccId +
      ",ccVerticesNum:" + ccVerticesNum +
      ", community:" + community +
      ", commVertexNum:" + commVertexNum +
      ", modularity:" + modularity +
      ", orderDate:" + orderDate
  }

}
package utils

/**
  * Created by meishangjian on 2017/5/8.
  */
class EdgeAttr extends Serializable {
  var relationVal: String = ""
  var relationType: String = ""
  var count: Int = 0

  override def toString: String = {
    val sep = DefaultValueUtils.FIELD_SEP
      relationVal + sep +
      relationType + sep +
      count
  }

}

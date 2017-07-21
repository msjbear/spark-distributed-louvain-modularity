package utils

/**
  * Created by meishangjian on 2017/5/8.
  */
object DefaultValueUtils {

  val FIELD_SEP = "\t"
  val strMissingVal = "missing"
  val countMissingVal = 0
  val vidMissingVal = -99
  val vidLongMissingVal: Long = -99L
  val intCountMissingVal: Int = 0
  val doubleCountMissingVal: Double = 0D
  val dateStrMissingVal = "1970-01-01"
  val vidMinValue = 0
  val floatMissingVal: Float = 0f
  val riskValueMissingVal: Float = 0f

  def getIntVal(v: String): Int = {
    if (v == null || v.equals("")) 0 else v.toInt
  }

  def getWeightVal(v: String): Int = {
    if (v == null || v.equals("") || v.trim.equals("0")) 1 else v.toInt
  }

  def getFloatVal(v: String): Float = {
    if (v == null || v.equals("")) 0f else v.toFloat
  }

  def getDoubleVal(v: Any): Double = {
    if (v == null || v.toString.equals("")) 0D else v.toString.toDouble
  }

  def getStringVal(v: Any): String = {
    if (v == null ) "" else v.toString
  }

  @deprecated
  def getLongVal(v: Any): Long = {
    if (v == null || v.toString.equals("")) 0L else v.toString.toLong
  }

  def getVidLongVal(v: String): Long = {
    if (v == null || v.equals("")) -1L else v.toLong
  }

  def getDateStringVal(v: Any): String = {
    if (v == null || v.toString.equals("")) "missing" else v.toString
  }

}

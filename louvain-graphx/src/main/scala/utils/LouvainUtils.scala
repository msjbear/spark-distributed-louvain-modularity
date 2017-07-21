package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by meishangjian on 2017/5/3.
  */
object LouvainUtils {

  def deleteFile(fileStr: String): Unit = {
    val conf: Configuration = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)
    val fileName: Path = new Path(fileStr)
    if (fs.exists(fileName)) {
      fs.delete(fileName, true)
    } else {
      println("JRDM:" + "file not exists, fileName: " + fileName)
    }
  }

}

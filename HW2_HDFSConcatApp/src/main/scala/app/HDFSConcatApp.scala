package app

import java.net.URI
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.io.FileNotFoundException


object HDFSConcatApp extends App {

  val namenodeURI: String = "hdfs://localhost:9000"
  val srcLayer = "stage"
  val dstLayer = "ods"

  val fs = FileSystem.get(new URI(namenodeURI), new Configuration())


  def createDir(fs: FileSystem, path: Path): Unit = {
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    } else {
      println(s"Directory '${path.getName}' exists")
    }
  }

  def createFile(fs: FileSystem, path: Path): Unit = {
    if (!fs.exists(path)) {
      fs.createNewFile(path)
    }
  }

  def removeFileOrFolder(fs: FileSystem, path: Path): Boolean = {
    fs.delete(path, true)
  }

  def getListStatus(fs: FileSystem, path: Path): Array[FileStatus] = {
    if (fs.exists(path)) {
      fs.listStatus(path)
    } else {
      throw new FileNotFoundException(s"File doesn't exist: ${path.getName}")
    }
  }

  createDir(fs, new Path(s"/$dstLayer"))

  val sourceDirs = getListStatus(fs, new Path(s"/$srcLayer"))
  sourceDirs.foreach(x => {
    println(x.getPath.toString)

    val dirFiles = getListStatus(fs, x.getPath).filter(_.getPath.getName.contains("csv")).filter(_.getLen > 0).map(_.getPath)
    dirFiles.foreach(y => println(y.toString))

    val mergedFilePath = new Path(s"/$srcLayer/${x.getPath.getName}/part.csv")
    createFile(fs, mergedFilePath)

    if (fs.exists(mergedFilePath)) {
      fs.concat(mergedFilePath, dirFiles.filter(_ != mergedFilePath))
    }

    val newPath = new Path(s"/$dstLayer/${x.getPath.getName}")
    createDir(fs, newPath)

    if (fs.rename(mergedFilePath, newPath)) {
      println(s"Ok: moving ${mergedFilePath} to ${newPath}")
    } else {
      println(s"Failed: moving ${mergedFilePath} to ${newPath}")
    }

    removeFileOrFolder(fs, new Path(s"/$srcLayer/${x.getPath.getName}"))

  })

}

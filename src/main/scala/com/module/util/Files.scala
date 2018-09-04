package com.module.util

import java.io._

/**
  * Miscellaneous file utilities.
  * They only work for the local file system, not Hadoop.
  */
object Files {

  case class FileOperationError(msg: String) extends RuntimeException(msg)

  def rmrf(root: String): Unit = rmrf(new File(root))

  def rmrf(root: File): Unit = {
    if (root.isFile) root.delete()
    else if (root.exists) {
      root.listFiles.foreach(rmrf)
      root.delete()
    }
  }

  def rm(file: String): Unit = rm(new File(file))

  def rm(file: File): Unit =
    if (!file.delete) throw FileOperationError(s"Deleting $file failed!")

  def mkdir(path: String): Unit = new File(path).mkdirs
}

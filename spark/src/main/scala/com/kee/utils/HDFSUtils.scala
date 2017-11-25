package com.kee.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging

object HDFSUtils extends Logging {
    val conf = new Configuration()

    def deleteIfExist(uri: String): Unit = {
        if (null != uri && isExist(uri)) {
            log.warn(s"File exists [$uri] which will be deleted first.")
            delete(uri)
        }
    }

    def deleteIfExist(uris: Seq[String]): Unit = uris.foreach(deleteIfExist)

    def isExist(uri: String): Boolean = {
        if (null == uri || uri.isEmpty) {
            log.error(s"Invalid(empty or null) path")
            return false
        }
        val path = new Path(uri)
        val fs = path.getFileSystem(conf)
        fs.exists(path)
    }

    def delete(uri: String): Boolean = {
        val fs = new Path(uri).getFileSystem(conf)
        fs.delete(new Path(uri), true)
    }

}

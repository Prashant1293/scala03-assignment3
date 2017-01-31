/**
  * First Part
  * •	Read files from a directory
  * •	For each file read contents of the file
  * •	Capitalize the contents of the file
  * •	Then write the capitalized content into another output file with the same name in a different directory
  *
  */

import java.io._

import scala.io._
abstract class Database {
  def databaseSource : String
  def databaseSink : String
}

abstract class Application{
  def fileSource : Database
  def fileSink : Database
}

object ETLProcessDatabase extends Database{
 val databaseSource = "C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\src\\"
  val databaseSink = "C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\OutputFolder\\"
  val file = "RegularExp.scala"

  def getFiles(dir: String, extensions: List[String]): List[File] = {
    val directory = new File(dir)
    if (directory.exists && directory.isDirectory) {
      directory.listFiles.filter(_.isFile).toList.filter({ file =>
        extensions.exists(file.getName.endsWith(_))
      })
    } else {
      List[File]()
    }
  }

  def readFiles(fileList: List[File]) {
    for (filename <- fileList) {
      for (line <- Source.fromFile(filename).getLines()) {
        println(line)
      }
    }
  }

  def capitalizeFile(fileList: List[File]): String = {
    var lines = ""
    for (filename <- fileList) {
      for (line <- Source.fromFile(filename).getLines()) {
        lines += line.toUpperCase
      }
    }
    lines
  }

  def storeInDatabase(content : Any) = {
    val writer = new PrintWriter(new File(databaseSink+file))
    writer.write(content.toString())
    writer.close()
  }

}

object ETLProcessApplication extends Application{
 val fileSource = ETLProcessDatabase
  val fileSink = ETLProcessDatabase
  val files = ETLProcessDatabase.getFiles(fileSource.databaseSource, List("scala"))
  val fileContent = ETLProcessDatabase.readFiles(files)
  val filesInCapital = ETLProcessDatabase.capitalizeFile(files)
  def updateDB = {
    ETLProcessDatabase.storeInDatabase(files.toString + fileContent.toString + filesInCapital.toString)
  }
}

object ProcessingAndLoadingDatabase extends Database{
  val databaseSource = "C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\src\\"
  val databaseSink = "C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\OutputFolder\\"
  val file = "RegularExp.scala"

  def processWords(file: String) : scala.collection.mutable.Map[String,Int] = {
    val map = scala.collection.mutable.Map[String,Int]()
    val src = Source.fromFile(file)
    for(line <- src.getLines){
      val words = line.split("\\s+")
      words.foreach(word =>
        if(map.contains(word.toLowerCase())){
          val value = map.filter(_._1 == word.toLowerCase()) map (_._2 + 1)
          map -= word.toLowerCase()
          map += (word.toLowerCase() -> value.head)
        }
        else
          map += (word.toLowerCase() -> 1)
      )
    }
    map
  }

  def fetchUniqueWords(file : String) = {
    val map = processWords(file)
    val list = map.filter(_._2 == 1).map(_._1)
    list
  }

 def wordCount(file : String) = {
    val map = processWords(file)
     val wordCountList = for{
                            item <- map
                            string = item._1 +":"+ item._2
                        }yield string
   println(wordCountList)
    wordCountList
  }

  def storeInDatabase(content : Any) = {
    val writer = new PrintWriter(new File(databaseSink+file))
    writer.write(content.toString())
    writer.close()
  }
}

object ProcessingAndLoadingApplication extends Application{
  val fileSource = ProcessingAndLoadingDatabase
  val fileSink = ProcessingAndLoadingDatabase
  val uniqueWords = ProcessingAndLoadingDatabase.fetchUniqueWords(fileSource.databaseSource+"RegularExp.scala")
  val countWords = ProcessingAndLoadingDatabase.wordCount(fileSource.databaseSource+"RegularExp.scala")
  def updateDB = {
    ProcessingAndLoadingDatabase.storeInDatabase(uniqueWords.toString + countWords.toString)
  }
}

object etl extends App{
  println(ProcessingAndLoadingApplication.uniqueWords)
  ProcessingAndLoadingApplication.countWords
  ProcessingAndLoadingApplication.updateDB
  println(ETLProcessApplication.files)
  println(ETLProcessApplication.fileContent)
  println(ETLProcessApplication.filesInCapital)
  ETLProcessApplication.updateDB
}

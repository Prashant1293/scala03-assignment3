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
  //override def toString: String = databaseSource
}

abstract class Application{
  val database : String
}

object ETLProcessDatabase extends Database{
  val databaseSource = "C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\src\\RegExp.scala"
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

}

object ETLProcessApplication extends Application{
  val database = "C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\src"
  val file = new File("C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\src\\RegExp.scala")
  val files = ETLProcessDatabase.getFiles(database, List("scala"))
  for (file <- files) println(file)
  ETLProcessDatabase.readFiles(files)
  val lines = ETLProcessDatabase.capitalizeFile(files)
  println(lines)
  val writer = new PrintWriter(new File("C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\src\\write.scala"))
  writer.write(lines)
  writer.close()
  Source.fromFile("C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\src\\Write.scala").foreach { x => print(x) }
}
object ProcessingAndLoadingDatabase extends Database{

  val databaseSource = "C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\src\\RegExp.scala"
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

  def printUniqueWords(file : String) = {
    val map = processWords(file)
    map.filter(_._2 == 1).map(_._1).foreach(x => println(x))
  }

  def wordCount(file : String) = {
    val map = processWords(file)
    map.foreach(x => println(s"${x._1} : ${x._2}"))
  }
}


object ProcessingAndLoadingApplication extends Application{
  val database = "C:\\Users\\Neha Bhardwaj\\IdeaProjects\\Assign2\\src\\RegExp.scala"
  ProcessingAndLoadingDatabase.printUniqueWords(database)
  ProcessingAndLoadingDatabase.wordCount(database)
}

object etl extends App{
  ProcessingAndLoadingApplication
  ETLProcessApplication
}

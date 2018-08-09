package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

object SetSimJoin {
  def main(args:Array[String]){
    val inputFile = args(0)
    val outputFolder = args(1)
    val threshold = args(2)
    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split("\n"))    
    val sort = words.map(word => (word.drop(word.split(" ")(0).length.toInt+1)))
    val sorted = sort.flatMap(x=>x.split(" "))
    //Build a dictionary to store the results of word frequency count
    var A:Map[Int,Int]= Map()
    val sorted3 = sorted.map(word => (word,1)).reduceByKey(_+_).map(x=>(x._1.toInt,x._2))
    sorted3.collect().foreach(x=>A+=(x._1->x._2))
    val find = words.map(x=>(x.split(" ")(0),x.drop(x.split(" ")(0).length.toInt+1)))
    //Sort the frequency
    implicit val FirstSort = new Ordering[(Int,Int)]{
      override def compare(a:(Int,Int),b:(Int,Int)):Int={
        if(a._2==b._2){
          a._1.compare(b._1)
        }else{
          a._2.compare(b._2)
        }        
      }
    }
    //Build an array-buffer to store each record's token and its frequency
    //format is (recordId, [(token,frequency)...])
    val find1 = find.map(x=> {var arrayfre = new ArrayBuffer[(Int,Int)]()
      for(i<-0 until x._2.split(" ").length.toInt)
      arrayfre += ((x._2.split(" ")(i).toInt,A.get(x._2.split(" ")(i).toInt).get))
    (x._1,arrayfre.sortBy(x=>x).map(x=>x._1))})
    //use the string buffer to get the string format of sorted record
    val finding = find1.map(x=>{var buf = new StringBuilder;
      for(i<-0 until x._2.length)
        {buf.append(x._2(i))
        buf++=" "
        }
        (x._1,buf.toString.dropRight(1))}).map(x=>x._1+" "+x._2)

    


    //compute prefix length p = len(line)-upbond(threshold*len(line))+1
    //map(eid,rid eid eid ...)
    val prefix = finding.flatMap(line => line.split("\n")).flatMap(x=>{for(i<-1 until 
        (x.split(" ").length-1)-(math.ceil((x.split(" ").length-1)*(threshold.toDouble))).toInt+2) 
      yield (x.split(" ")(i),x)})
    //Second sort is for the final output format
    implicit val SecondSort = new Ordering[(Int,Int,Double)]{
      override def compare(a:(Int,Int,Double),b:(Int,Int,Double)):Int={
        if(a._1==b._1){
          a._2.compare(b._2)
        }else{
          a._1.compare(b._1)
        }        
      }
    }
    //do filter steps to cut the pair not satisfy the requirement
    //RecordID1 < RecordID2
    //Use the ppjoin method in the paper, the max length of two records should longer or equal than
    //threshold/(1+threshould)*(record1.len +record2.len)
    val join = prefix.join(prefix).filter(x=>x._2._1.split(" ")(0).toInt<x._2._2.split(" ")(0).toInt).filter(x=>
      math.min(x._2._1.drop(x._2._1.split(" ")(0).length+1).split(" ").length.toInt,x._2._2.drop(x._2._2.split(" ")(0).length+1).split(" ").length.toInt)>=
          math.ceil(threshold.toDouble/(1+threshold.toDouble)*(
              (x._2._1.drop(x._2._1.split(" ")(0).length+1).split(" ").length.toInt+
                  x._2._2.drop(x._2._2.split(" ")(0).length+1).split(" ").length.toInt)))).
                  map(x=>((x._2._1.split(" ")(0),x._2._1.drop(x._2._1.split(" ")(0).length+1)),(
                      x._2._2.split(" ")(0),x._2._2.drop(x._2._2.split(" ")(0).length+1))))

    //compute Jaccard Similarity by the formula
    val zzz = join.map(x=>(x._1._1.toInt,x._2._1.toInt,x._1._2.split(" ").toSet.intersect(x._2._2.split(" ").toSet).size.toDouble/x._1._2.split(" ").toSet.union(
        x._2._2.split(" ").toSet).size.toDouble)).filter(x=>x._3>=threshold.toDouble)
    //delete the duplicate results and output
    val zhou = zzz.map(x=>(x._1.toString+" "+x._2.toString,x._3.toString)).reduceByKey(_+","+_).map(x=>(x._1.split(" ")(0).toInt,x._1.split(" ")(1).toInt,x._2.split(",")(0).toDouble))
    .sortBy(x=>x).map((x=>(x._1,x._2)+"\t"+x._3))


    zhou.saveAsTextFile(outputFolder)

  }
}
package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1 {
  def main(args:Array[String]){
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    //read each line
    val words = input.flatMap(line => line.split("\n"))
    //in order to compute the average length of the outgoing edges for each node
    //we take the information of FromNodeID and distance
    val value = words.map(x => (x.split(" ")(1).toInt,x.split(" ")(3).toDouble))
    type MVType = (Int, Double)
    //calculate the average
    val res =value.combineByKey(score =>(1,score), (c1:MVType,newScore)=>(c1._1+1,c1._2+newScore),
        (c1:MVType,c2:MVType)=>(c1._1 +c2._1,c1._2+c2._2))
        .map{case (node,(num,total))=>(node,total/num)}
    //secondary sort
    implicit val SecondSort = new Ordering[(Int,Double)]{
      override def compare(a:(Int,Double),b:(Int,Double)):Int={
        if(a._2==b._2){
          a._1.compare(b._1)
        }else{
          a._2.compare(b._2)
        }        
      }
    }
    val sort = res.map(x=>((-1)*x._1,x._2)).sortBy(x=>x,false)
    val result = sort.map(x => (-1)*x._1+"\t"+x._2)


    result.saveAsTextFile(outputFolder)

  }
}
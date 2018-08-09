package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Problem2 {
  def main(args: Array[String]){
    val inputFile = args(0)
    val node = args(1)
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    //read the file
    val words = input.flatMap(line => line.split("\n"))
    //build the graph
    val edgeFile:RDD[String] = words
    val edge = edgeFile.map{e=>
      val fields = e.split(" ")
      Edge(fields(1).toLong,fields(2).toLong,fields(3).toDouble)}
    val vertex = edgeFile.map{e=>
      val fields = e.split(" ")
      (fields(1).toLong,fields(1))
    }
    val graph = Graph(vertex,edge,"").persist()
    //query nodeId

    val sourceId: VertexId = node.toLong
    //use the thought of sssp in lecture notes, count the value is not infinite
    val initialGraph = graph.mapVertices((id,_)=>
      if(id==sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
        (id,dist,newDist)=>math.min(dist,newDist),
        triplet=>{
          if(triplet.srcAttr + triplet.attr < triplet.dstAttr){
            Iterator((triplet.dstId,triplet.srcAttr + triplet.attr))
          }else{
            Iterator.empty
          }
        },
        (a,b)=>math.min(a,b)
        )
    val result = sssp.vertices.filter(line=>line._2.!=(Double.PositiveInfinity)).count()-1
    println(result)
  }
}
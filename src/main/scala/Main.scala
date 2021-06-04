import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession


object Main extends App {

//  def bronKerboschl(R:Set[VertexId], P:Set[VertexId], X:Set[VertexId], map:Map[VertexId, Set[VertexId]]): Unit ={
//    if(P.toList.length ==0 && X.toList.length ==0){
//      buf += R
//    }else {
//      for (v <- P) {
//        var Nv: Set[VertexId] = map.get(v).get
//        bronKerboschl(R+v, P.intersect(Nv), X.intersect(Nv), map)
//        X += v
//        P -= v
//      }
//    }
//  }

  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

  val file_path = "graphDatasets/musae_git_edges.csv"
  val edges = spark.read.option("header",true).csv(file_path).rdd
  val edges_ = edges.map(row => Edge(row.getString(0).toLong, row.getString(1).toLong, ""))

  val file_path_v = "graphDatasets/musae_git_target.csv"
  val vertices = spark.read.option("header",true).csv(file_path_v).rdd
  val vertices_ = vertices.map(row => (row.getString(0).toLong, (row.getString(1), row.getString(2).toLong)))

  val graph = Graph(vertices_, edges_)
//  var R = Set[VertexId]()
//  var P = Set[VertexId]()
//  graph.vertices.collect().foreach(t=>{P+=t._1})
//  var X = Set[VertexId]()
//
//  val buf = scala.collection.mutable.ListBuffer.empty[Set[VertexId]]
//
//  val map: Map[VertexId, Set[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either).collect()
//    .map(t => {
//      var set: mutable.Set[VertexId] = Set[VertexId]()
//      t._2.foreach(t=>{set+=t})
//      (t._1, set)
//    }).toMap

  val startTime = System.currentTimeMillis
  val res = graph.triangleCount().vertices.collect()
  val estimatedTime = (System.currentTimeMillis - startTime) / 1000.0
  println("Time: " + estimatedTime + " s")
  //res.foreach { println }
  println(res.map(_._2).sum / 3)

}
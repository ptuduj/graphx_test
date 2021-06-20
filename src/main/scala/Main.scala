import org.apache.spark.graphx.lib.TriangleCount
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession


object Main extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

  val file_path = "graphDatasets/musae_facebook_edges.csv"
  val edges = spark.read.option("header",true).csv(file_path).rdd
  val edges_ = edges.map(row => Edge(row.getString(0).toLong, row.getString(1).toLong, ""))

  val file_path_v = "graphDatasets/musae_facebook_target.csv"
  val vertices = spark.read.option("header",true).csv(file_path_v).rdd
  val vertices_ = vertices.map(row => (row.getString(0).toLong, (row.getString(1))))

  //val vertices_ = vertices.map(row => (row.getString(0).toLong, (row.getString(1), row.getString(2).toLong)))

  val graph = Graph(vertices_, edges_)
  val startTime1 = System.currentTimeMillis
  //  val res = graph.triangleCount().vertices.collect()
  //  res.foreach(println)
  val res = TriangleCount.runPreCanonicalized(graph).vertices.collect()
  println("hau Time: " + estimatedTime + " s")

//  val vertice_df = spark.createDataFrame(vertices_).toDF("id", "name")
//  val edges_df = spark.createDataFrame(edges_).toDF("src", "dst", "attr")
//  val g = GraphFrame(vertice_df, edges_df)
//  val startTime = System.currentTimeMillis
//  val res = g.triangleCount.run().collect()
//  val estimatedTime = (System.currentTimeMillis - startTime) / 1000.0
//  println("kot Time: " + estimatedTime + " s")


}
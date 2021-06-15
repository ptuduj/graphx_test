import org.apache.spark.graphx.Edge
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame


object Main extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

  val file_path = "graphDatasets/com-lj.ungraph.csv"
  val edges = spark.read.option("header",true).option("delimiter","\t").csv(file_path).rdd
  val edges_ = edges.map(row => Edge(row.getString(0).toLong, row.getString(1).toLong, ""))

  val file_path_v = "graphDatasets/lj_target.csv"
  val vertices = spark.read.option("header",true).csv(file_path_v).rdd
  val vertices_ = vertices.map(row => (row.getString(0).toLong, (row.getString(1))))


  val vertice_df = spark.createDataFrame(vertices_).toDF("id", "name")
  val edges_df = spark.createDataFrame(edges_).toDF("src", "dst", "attr")
  val g = GraphFrame(vertice_df, edges_df)
  val startTime = System.currentTimeMillis
  val res = g.triangleCount.run().collect()
  val estimatedTime = (System.currentTimeMillis - startTime) / 1000.0
  println("Time: " + estimatedTime + " s")


}
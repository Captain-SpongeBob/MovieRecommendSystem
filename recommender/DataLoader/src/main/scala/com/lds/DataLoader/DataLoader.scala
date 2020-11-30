package com.lds.DataLoader

import java.net.InetAddress

import com.mongodb.casbah.Imports.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

// 定义样例类
case class Movie(mid: Int, name: String, descri: String, timelong: String,issue: String,shoot: String, language: String, genres:String, actors: String,directors: String)
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)
case class MongoConfig(uri:String, db:String)
case class ESConfig(httpHosts:String, transportHosts:String, index:String,clustername: String)

object DataLoader {
  val MOVIE_DATA_PATH = "C:\\dev\\Progam Files\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "C:\\dev\\Progam Files\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "C:\\dev\\Progam Files\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"
  //定义配置参数
  val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://localhost:27017/recommender_movie",
    "mongo.db" -> "recommender_movie",
    "es.httpHosts" -> "localhost:9200",
    "es.transportHosts" -> "localhost:9300",
    "es.index" -> "recommender_movie",
    "es.cluster.name" -> "elasticsearch")

  def main(args: Array[String]): Unit = {
    // 创建一个SparkConf配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 在对DataFrame和Dataset进行操作许多操作都需要这个包进行支持
    import spark.implicits._
    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    // 声明了一个ES配置的隐式参数
    implicit val esConfig = ESConfig(config("es.httpHosts"),
      config("es.transportHosts"),
      config("es.index"),
      config("es.cluster.name"))

    // 将Movie、Rating、Tag数据集加载进来
    val movieRDD: RDD[String] = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    val tagRDD: RDD[String] = spark.sparkContext.textFile(TAG_DATA_PATH)

    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    //TODO 将数据保存到MongoDB中
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    import org.apache.spark.sql.functions._

    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag"))
        .as("tags"))
      .select("mid", "tags")
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid", "mid"), "left")

    //TODO 需要将新的Movie数据保存到ES中
   // storeDataInES(movieWithTagsDF)
    // 关闭Spark
    spark.stop()

  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame) = {
    val mongoClient = MongoClient(MongoClientURI(config("mongo.uri")))
    mongoClient(config("mongo.db"))(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(config("mongo.db"))(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(config("mongo.db"))(MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入MongoDB
    movieDF
      .write
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF
      .write
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF
      .write
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(config("mongo.db"))(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(config("mongo.db"))(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(config("mongo.db"))(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(config("mongo.db"))(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(config("mongo.db"))(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    //关闭MongoDB的连接
    mongoClient.close()

  }

  def storeDataInES(movieDF: DataFrame)(implicit eSConfig: ESConfig): Unit ={
    // 新建es配置
    val settings: Settings = Settings.builder().put("cluster.name", eSConfig.clustername).build()

    // 新建一个es客户端
    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress( InetAddress.getByName(host), port.toInt ))
      }
    }

    // 先清理遗留的数据
    if( esClient.admin().indices().exists( new IndicesExistsRequest(eSConfig.index) )
      .actionGet()
      .isExists
    ){
      esClient.admin().indices().delete( new DeleteIndexRequest(eSConfig.index) )
    }

    esClient.admin().indices().create( new CreateIndexRequest(eSConfig.index) )

    movieDF.write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_MOVIE_INDEX)
  }

}

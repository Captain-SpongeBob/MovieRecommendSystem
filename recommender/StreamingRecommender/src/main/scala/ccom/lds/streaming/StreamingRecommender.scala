package ccom.lds.streaming

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.Seconds
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._
// 连接助手对象
object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender_movie"))
}
case class MongoConfig(uri: String, db: String)
//标准的推荐对象
case class Recommendation(mid: Int, score: Double)
//用户推荐对象
case class UserRecs(uid: Int, seq: Seq[Recommendation])
//电影相似度(与该电影相似的电影列表）
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object StreamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"
  val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://localhost:27017/recommender_movie",
    "mongo.db" -> "recommender_movie"
  )

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    //广播电影相似度矩阵
    // 装换成为Map[Int, Map[Int,Double]]
    val simMoviesMatrix = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_RECS_COLLECTION )
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map(recs =>
        (recs.mid, recs.recs.map(x => (x.mid,x.score)).toMap)
        )
      .collectAsMap()
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

    //创建到kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender_movie",
      "auto.offset.reset" -> "latest"
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))
    // UID|MID|SCORE|TIMESTAMP// 产生评分流
    val ratingSteam: DStream[(Int, Int, Double, Int)] = kafkaStream.map{
      case msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }

    //核心实施推荐算法
    ratingSteam.foreachRDD{
      rdd =>
        rdd.map{
          case (uid, mid, score, timestamp) =>
            println(">>>>>>>>>>>")
            //获取当前最近的M次电影评分
            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid,ConnHelper.jedis)
            //获取电影P最相似的K个电影
            val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMoviesMatrixBroadCast.value)
            //计算待选电影的推荐优先级
            val streamRecs = computeMovieScores(simMoviesMatrixBroadCast.value,userRecentlyRatings,simMovies)
            // 将数据保存到MongoDB
            saveRecsToMongoDB(uid,streamRecs)
        }.count()
      //启动Streaming程序
      ssc.start()
      ssc.awaitTermination()
    }
  }

  /*** 获取当前最近的M次电影评分
   * @param num  评分的个数
   * @param uid  谁的评分* @return
   * */
  def getUserRecentlyRating(num:Int, uid:Int,jedis:Jedis): Array[(Int,Double)] = {
    //从用户的队列中取出num个评分
    jedis.lrange("uid:"+uid.toString, 0, num)
      .map{item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }.toArray
    }

}

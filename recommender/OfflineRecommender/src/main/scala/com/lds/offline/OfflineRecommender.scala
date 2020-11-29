package com.lds.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.jblas.DoubleMatrix

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)
case class MoiveRating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class MongoConfig(uri: String, db: String)
//标准的推荐对象
case class Recommendation(mid: Int, score: Double)
//用户推荐对象
case class UserRecs(uid: Int, seq: Seq[Recommendation])
//电影相似度(与该电影相似的电影列表）
case class MovieRecs(mid: Int, recs: Seq[Recommendation])
object OfflineRecommender {


  // 定义常量val
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  // 推荐表的名称
  val USER_RECS= "UserRecs"
  val MOVIE_RECS= "MovieRecs"
  val USER_MAX_RECOMMENDATION= 20
  // 定义配置
  val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://localhost:27017/recommender_movie",
    "mongo.db" -> "recommender_movie")
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    /**
     * 评分的数据集
     * Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
     */
    val ratingDS: Dataset[(Int, Int, Double)] = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION )
      .format("com.mongodb.spark.sql")
      .load()
      .as[MoiveRating]
      .map(rating => (rating.uid, rating.mid, rating.score))
      .cache()
    // 用户的数据集
    val userDS: Dataset[Int] = ratingDS.map(_._1).distinct()
    /**
     * 电影的数据集
     * 、Movie(
     *    mid: Int, name: String, descri: String, timelong: String,issue: String,
     *    shoot: String, language: String, genres:String, actors: String,directors: String
     *    )
     */
    val movieDS = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION )
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(_.mid)
      .cache()
    //创建训练数据集
    val trainData: RDD[Rating] = ratingDS.map(x => Rating(x._1, x._2, x._3)).rdd
    //rank 是模型中隐语义因子的个数, iterations 是迭代的次数, lambda 是ALS的正则化参
    val (rank,iterations,lambda) = (50, 5, 0.01)
    //调用ALS算法训练隐语义模型
    val model = ALS.train(trainData,rank,iterations,lambda)
    //计算用户推荐矩阵
    val userMovies= userDS.rdd.cartesian(movieDS.rdd)
    // model已训练好，把id传进去就可以得到预测评分列表RDD[Rating] (uid,mid,rating)
    val preRatings: RDD[Rating] = model.predict(userMovies)
    val userRecs: DataFrame = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map{
        case (uid, recs) => UserRecs(uid, recs
          .toList.sortWith(_._2 > _._2)
          .take(USER_MAX_RECOMMENDATION)
          .map(x => Recommendation(x._1,x._2)))
      }
      .toDF()

    userRecs
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //TODO：计算电影相似度矩阵
    //计算电影相似度矩阵//获取电影的特征矩阵，
    // 数据格式RDD[(scala.Int, scala.Array[scala.Double])]
    val movieFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map{
      case (mid,features) =>(mid, new DoubleMatrix(features))
    }
    // 计算笛卡尔积并过滤合并
    val movieRecs = movieFeatures
      .cartesian(movieFeatures)
      .filter{
        case (a,b) => a._1 != b._1
      }
      .map{
        case (a,b) =>
          val simScore = this.consinSim(a._2,b._2)
          // 求余弦相似度
          (a._1,(b._1,simScore))
      }
      .filter(_._2._2 > 0.6)
      .groupByKey()
      .map{
        case (mid,items) => MovieRecs(mid,items.toList.map(x => Recommendation(x._1,x._2)))
      }
      .toDF()

    movieRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()



    // 关闭sparkspark.stop()
  }
  //计算两个电影之间的余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2:DoubleMatrix) : Double ={
    movie1.dot(movie2) / ( movie1.norm2()  * movie2.norm2() )
  }
}

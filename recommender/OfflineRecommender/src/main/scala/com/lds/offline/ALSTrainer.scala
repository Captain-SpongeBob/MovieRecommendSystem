package com.lds.offline

import breeze.numerics.sqrt
import com.lds.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object ALSTrainer {

  def main(args: Array[String]): Unit = {
    //定义配置
    val config: Map[String, String] = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender_movie",
      "mongo.db" -> "recommender_movie"
    )

    //创建SparkSession
    val sparkConf = new SparkConf().setMaster(config("spark.core")).setAppName("OfflineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    val ratingDS = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .map(rating => Rating(rating.uid, rating.mid, rating.score))
      .cache()

    //划分训练集和测试集
    val splits:Array[Dataset[Rating]]= ratingDS.randomSplit(Array(0.8, 0.2))
    val trainData:Dataset[Rating] = splits(0)
    val testData:Dataset[Rating] = splits(1)

    //输出最优参数
    adjustALSParams(trainData.rdd, testData.rdd)

    //关闭Spark
    spark.close()
  }

  def getRMSE(model:MatrixFactorizationModel, data:RDD[Rating]):Double={
    val userProducts = data.map(item => (item.user,item.product))
    val predictRating = model.predict(userProducts)
    val real = data.map(item => ((item.user,item.product),item.rating))
    val predict = predictRating.map(item => ((item.user,item.product),item.rating))
    // 计算RMSE
    sqrt(
      real.join(predict).map{
        case ((userId,productId),(real,pre))=>
        // 真实值和预测值之间的差
        val err = real - pre
        err * err
      }.mean()
    )
  }

  def adjustALSParams(trainData:RDD[Rating], testData:RDD[Rating]) ={
    // rank和lambda在几个值中选取调整
    //当循环结束后, 会返回所有 yield 的值组成的集
    val result:Array[(Int, Double, Double)] =
    for(rank <- Array(100,200,250); lambda <- Array(1, 0.1, 0.01, 0.001)) yield {
        val model:MatrixFactorizationModel = ALS.train(trainData,rank,5,lambda) //这里指定迭代次数为5，
      //均方根误差（RMSE），考察预测评分与实际评分之间的误差。
        val rmse = getRMSE(model, testData)
        (rank,lambda,rmse)
      }
    // 按照rmse排序
    println(result.sortBy(_._3).head)

  }

}

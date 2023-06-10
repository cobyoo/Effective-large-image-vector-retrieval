//2022.04.20 created by ysh 

package com.example.clustering

/**
 * @author ${user.name}
 */
import org.apache.spark.rdd.RDD // RDD 형식 파일
import org.apache.spark._
import org.apache.spark.sql.{Row,SparkSession,SQLContext,DataFrame,Encoders}
import org.apache.spark.sql.functions.lit
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.types.{DataTypes,StructType, StructField, StringType, IntegerType, FloatType, MapType, DoubleType, ArrayType}
import org.apache.spark.sql.functions.{col,from_json,split,explode,abs,exp,pow,sqrt,broadcast}
import scala.collection.mutable.Map
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.collection.immutable._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import java.util.ArrayList
import scala.collection.mutable
import scala.util.control.Breaks._
import java.math.MathContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.mllib.recommendation.Rating

object App {
  case class ImageFeature(id: Long, approxFeatures: Seq[Vector], actualFeatures: Vector)

  def approximateFeatures(features: Vector): Seq[Vector] = {
    // 특징 벡터를 이용하여 근사화된 벡터를 생성
    val approxFeature = Vectors.dense(features.toArray.map {
      case value if value >= 0.0 && value <= 0.4 => 0.0
      case value if value > 0.4 && value <= 0.9 => 1.0
      case value => value
    })
    // 생성된 근사화된 벡터를 반환
    Seq(approxFeature)
  }
  // 유사도 계산 함수
  def compareFeatures(imageFeatures: Vector, queryFeatures: Vector): Double = {
    val gamma = 0.5
    val sum = imageFeatures.toArray.zip(queryFeatures.toArray).map {
      case (p1, p2) =>
        val d = p2 - p1
        math.pow(d, 2)
    }.sum
    math.exp(-gamma / sum)
  }

  def main(args: Array[String]): Unit = {
    // Spark 세션 생성
    val spark = SparkSession.builder()
      .appName("SimilaritySearch")
      .getOrCreate()

    import spark.implicits._

    // 이미지 특징 벡터 데이터를 CSV 파일에서 로드합니다.
    val rawDataPath = "path/to/your/raw_data.csv"
    val rawDataRDD = spark.sparkContext.textFile(rawDataPath).map { line =>
      val tokens = line.split(',')
      (tokens(0).toLong, Vectors.parse(tokens(1)))
    }
    val rawData = rawDataRDD.toDF("id", "features")

    // 2단계 검색에서 1단계 이산화된 코드 전의 데이터
    val imageFeatures = rawData.as[(Long, Vector)].map {
      case (id, features) =>
        val approxFeatures = approximateFeatures(features)
        ImageFeature(id, approxFeatures, features)
    }

    // 비교하고자 하는 이미지 특징 벡터를 CSV 파일에서 로드합니다.
    val queryFeaturesPath = "path/to/your/query_features.csv"
    val queryFeaturesString = spark.sparkContext.textFile(queryFeaturesPath).first()
    val queryFeatures = Vectors.parse(queryFeaturesString)


    val k = 3 // 근접 이웃 개수
    var start = System.nanoTime() // 시작 시간 측정

    // 유사도 계산 후 결과를 DataFrame으로 저장
    val similarities = imageFeatures.map {
      case ImageFeature(id, features, _) =>
        val similarity = features.map(approx => compareFeatures(approx, queryFeatures)).max
        (id, similarity)
    }.toDF("id", "similarity")

    var end = System.nanoTime() // 종료 시간 측정

    // 유사도 기준 상위 k개의 결과 선택
    val topKImages = similarities.orderBy($"similarity".desc).take(k)

    val duration = (end - start) // 소요 시간 계산 (초 단위)
    println("\n\n")
    println("==========================================================================================")
    println("                                      similarities                                        ")
    println("==========================================================================================")
    // 결과 출력
    topKImages.foreach { row =>
      println(s"ID: ${row.getAs[Long]("id")}, Similarity: ${row.getAs[Double]("similarity")}")
    }  
    println("Completed")
    println("\n\n")
    println("==========================================================================================")
    println("                                      Time Taken                                          ")
    println("==========================================================================================")
    println(s"Time Taken: ${NANOSECONDS.toNanos(end-start)}ns")
    println("\n\n")

    // Spark 종료
    spark.stop()
  }
}

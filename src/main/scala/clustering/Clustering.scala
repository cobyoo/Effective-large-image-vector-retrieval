//2022.04.20 created by ysh 

import scala.io.Source
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import scala.util.control._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD // RDD 형식 파일
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus} //hadoop FileSystem API
import org.apache.spark._
import org.apache.hadoop.fs._
import org.apache.spark.sql.{Row,SparkSession,SQLContext,DataFrame}
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
import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.recommendation.Rating

object Clustering {
    def main(args: Array[String]) : Unit = {
         val spark:SparkSession = SparkSession.builder()
                                                .master("yarn")
                                                .appName("rbf_cluster")
                                                .getOrCreate()
        val sc = spark.sparkContext
        //val sqlContext = new org.apache.spark.sql.SQLContext(sc) //데이터 프레임
        //sqlContext.setConf("spark.sql.shuffle.partitions","40")
        //import sqlContext.implicits._ // 데이터 프레임 라이브러리       

        val feature_path_query = "/wedrive_data/raw_data/query_row.csv" // 이미지 벡터 파일 
        //val feature_path_whole = "/wedrive_data/raw_data/feature_row.csv"// 만 개 이미지 벡터 파일 
        val feature_path_whole = "/wedrive_data/raw_data/millon.csv"// 백만 개 이미지 벡터 파일 
        val img_list_query = data_process(feature_path_query, sc).flatMap(_._2)
        val img_list_whole = data_process(feature_path_whole, sc)

        val point = 1000
        //val list = img_list_query.collect.map(x => ((x * point).toInt))
        val list = img_list_query.collect.map(x => x)
        //시간 측정
        var start = System.nanoTime() //나노 초로 시간 측정 
        
        val res = img_list_whole.map { line => 
        //val data = line._2.map(x => ((x * point).toInt))
        val data = line._2.map(x => x)
        val data_res = rbf_kernel(data, list)
        (line._1 , data_res)
        } 
        
        println("\n\n")
        var end = System.nanoTime()  // 나노 초로 끝 시간 측
        res.sortBy(_._2).take(10).foreach(println) 
        println("\n\n")
        println("==========================================================================================")
        println("                                      RBF Kernel                                          ")
        println("==========================================================================================")     
        println("Completed")
        println("\n\n")
        println("==========================================================================================")
        println("                                      Time Taken                                          ")
        println("==========================================================================================")
        println(s"Time Taken: ${NANOSECONDS.toNanos(end-start)}ns")
        println("\n\n")
        sc.stop()
    }

    def data_process (feature_file : String, sc : SparkContext) : RDD[(String , Array[Float])] = {   
       
        val slength = 4096

        val feature_RDD = sc.textFile(feature_file, minPartitions=8) //RDD생성

        val list = feature_RDD.map { x => 
        val spl = x.replace("\\[|\\]","")
                   .replace("(","")
                   .replace("))","")
                   .replace("Vector","").split(",")
        val key = spl(0)
        val fvals = spl.slice(1,slength).map(x => x.toFloat)    
        (key,fvals)
        }
        return list
    }

    def rbf_kernel(fv1 : Array[Float], fv2 : Array[Float]) = {
    // 0.1 , 0.5 , 10
    val gamma = 0.5
    val sum = fv1.zip(fv2).map {
            case (p1, p2) => 
                val d = p2 - p1
                scala.math.pow(d,2)
            }.sum 
    val res = scala.math.exp(-gamma / sum)
    res
    }
}

import scala.io.Source 
import scala.util.control._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD // RDD 형식 파일
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus} //hadoop FileSystem API
import org.json.JSONObject //java maven
// import org.apache.spark.sql.functions.to_json // json
// import hilbertcurve
// import org.apache.spark.sql.SQLContext //json file -> dataFrame 변환하기 위한 라이브러리
// import sqlContext.implicits._ //json file -> dataFrame 변환하기 위한 라이브러리


object pipe_processing {
     def main(agrs: Array[String]) { 
        // SparkContext 객체 초기화
        val conf = new SparkConf().setMaster("yarn").setAppName("My App") 
        val sc = new SparkContext(conf)
        val trajectory_hadoop_file_path = "/wedrive_data/raw_data/TB_TRACKING2_SIMPLE_20200102.sql.gz"
        val point_hadoop_file_path = "/wedrive_data/raw_data/TB_TRACKING2_DATA_20200102.sql.gz"
        val trajectory_pair_RDD = process_trajectory_file(trajectory_hadoop_file_path, sc)
        val point_RDD = process_point_file(point_hadoop_file_path, sc)
        // val joined_RDD = trajectory_pair_RDD.join(point_RDD)
        // val mapping_information_RDD = mapping_link_of_jsm(joined_RDD)
        // val completed_RDD = add_link_and_cell(joined_RDD, mapping_information_RDD)
        // println("\n result Start \n")
        // joined_RDD.take(3).foreach(x => println(x))
        // println("\n result End \n")
        
        println("\n result Start \n")
        println("\n trajectory_pair_RDD \n")
        trajectory_pair_RDD.take(3).foreach(x => println(x)) //RDD의 각 값에 function 적용
        println("\n point_RDD \n")
        point_RDD.take(3).foreach(x => println(x))
        println("\n joined_RDD \n")
        // joined_RDD.take(3).foreach(x => println(x))
        // println("\n result End \n")
    }
    // def process_trajectory_file (trajectory_hadoop_file_path : String, sc : SparkContext) : (String, List[String]) = {
    def process_trajectory_file (trajectory_hadoop_file_path : String, sc : SparkContext) : RDD[(String, Array[String])] = {
        val trajectory_RDD = sc.textFile(trajectory_hadoop_file_path)
        val insert_into_RDD = trajectory_RDD.filter(line => line.contains("INSERT INTO"))
        val values_RDD = insert_into_RDD.map(line => line.split("VALUES ")(1))
        val unit_trajectory_RDD = values_RDD.flatMap(line => line.split("\\),"))
        val stringcut_RDD = unit_trajectory_RDD.map(x => cut_string_trajectory(x))
        // println("\n result Start \n")
        // stringcut_RDD.take(3).foreach(x => println(Arrays.toString(x))
        // println("\n result End \n")
        stringcut_RDD
    }
    def process_point_file (point_hadoop_file_path : String, sc : SparkContext) : RDD[(String, Array[String])] = {
        val point_RDD = sc.textFile(point_hadoop_file_path)
        val insert_into_RDD = point_RDD.filter(line => line.contains("INSERT INTO"))
        val values_RDD = insert_into_RDD.map(line => line.split("VALUES ")(1))
        val items_RDD = values_RDD.map(line => line.split("items"))
        val unit_trajectory_RDD = values_RDD.flatMap(line => line.split("\\),"))
        val stringcut_RDD = unit_trajectory_RDD.map(x => cut_string_point(x))
        stringcut_RDD
    }
    def cut_string_trajectory (values_text : String) : (String, Array[String]) = {
        var cut_text = values_text
        cut_text.take(3).foreach(x => println(x))
        if (cut_text(0) == '('){
            cut_text = cut_text.slice(1,cut_text.length)
        }
        if (cut_text(cut_text.length-2) == ')'){
            cut_text = cut_text.slice(0,cut_text.length-2)
        }
        if (cut_text(cut_text.length-1) == ')'){
            cut_text = cut_text.slice(0,cut_text.length-1)
        }
        val value_list = cut_text.split(",")
        val uuid = value_list(0)
        val begin_time = value_list(1)
        val trajectory_pair = (begin_time + " " + uuid, value_list.slice(2, value_list.length-1))
        trajectory_pair
    }
    // def cut_string_point (values_text : String) : (String, Array[String]) = {
    //     var cut_text = values_text
    //      cut_text.take(3).foreach(x => println(x))
    //     if (cut_text(0) == '('){
    //         cut_text = cut_text.slice(1, cut_text.length)
    //     }
    //     if (cut_text(cut_text.length-1) == ';'){
    //         cut_text = cut_text.slice(0, cut_text.length-1)
    //     }
    //     if (cut_text(cut_text.length-1) == ')'){
    //         cut_text = cut_text.slice(0, cut_text.length-1)
    //     }
    //     val value_list = cut_text.split(",")
    //     val uuid = value_list(0)
    //     val begin_time = value_list(1)
    //     val parse_string_list = value_list.slice(2,value_list.length)
    //     var parse_string = json_string_list.subString(",")
    //     if (cut_text(0) == '['){
    //         cut_text = cut_text.slice(1, )
    //     }
    //     point_pair
    // }

}

















package sample
import com.alibaba.fastjson.JSON

object FastJsonTest {
  def main(args: Array[String]): Unit = {
    val jsonstr = "{\n\t\"also_like_9_url\": \"ERT\",\n\t\"after\": {\n\t\t\"id\": 15,\n\t\t\"title\": \"tt5\",\n\t\t\"author\": \"aa5\"\n\t},\n\t\"source\": {\n\t\t\"version\": \"0.7.5\",\n\t\t\"name\": \"mysqlclustertest\",\n\t\t\"server_id\": 1,\n\t\t\"ts_sec\": 1537588970,\n\t\t\"gtid\": null,\n\t\t\"file\": \"mysql-bin.000014\",\n\t\t\"pos\": 1675823,\n\t\t\"row\": 0,\n\t\t\"snapshot\": false,\n\t\t\"thread\": 20,\n\t\t\"db\": \"test\",\n\t\t\"table\": \"tbl\"\n\t},\n\t\"op\": \"c\",\n\t\"ts_ms\": 1537588969312\n}"
    println(JSON.parseObject(jsonstr, classOf[Tdl]))
  }
}
case class Tdl(id: Integer,title: String,author: String,op:String,tsMs:Long,alsoLike9Url:String)
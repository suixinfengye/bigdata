package sample

import com.alibaba.fastjson.JSON

object FastJsonTest {
  def main(args: Array[String]): Unit = {
    //    val jsonstr = "{\n\t\"also_like_9_url\": \"ERT\",\n\t\"after\": {\n\t\t\"id\": 15,\n\t\t\"title\": \"tt5\",\n\t\t\"author\": \"aa5\"\n\t},\n\t\"source\": {\n\t\t\"version\": \"0.7.5\",\n\t\t\"name\": \"mysqlclustertest\",\n\t\t\"server_id\": 1,\n\t\t\"ts_sec\": 1537588970,\n\t\t\"gtid\": null,\n\t\t\"file\": \"mysql-bin.000014\",\n\t\t\"pos\": 1675823,\n\t\t\"row\": 0,\n\t\t\"snapshot\": false,\n\t\t\"thread\": 20,\n\t\t\"db\": \"test\",\n\t\t\"table\": \"tbl\"\n\t},\n\t\"op\": \"c\",\n\t\"ts_ms\": 1537588969312\n}"
    val jsonstr = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false," +
      "\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"title\"},{\"type\":\"string\"," +
      "\"optional\":true,\"field\":\"author\"}],\"optional\":true,\"name\":\"mysqlfullfillment.test.tbl.Value\"}," +
      "\"payload\":{\"id\":null,\"title\":\"tt20\",\"author\":null}}"

    //    val jsonObject = JSON.parseObject(jsonstr);
    //    val data = jsonObject.getString("payload");
    //    val schema = JSON.parseObject(jsonObject.getString("schema"));
    //    val topicName = schema.getString("name").split("\\.")
    //    val t = topicName(topicName.size - 2)
    //    println(topicName.toString)
    //    println(t)
    //    println(data)
    //    println(JSON.parseObject(data, classOf[Tdl]))

    val aa = "{\n\t\"id\":null,\n\t\"title\": \"title\",\n\t\"author_name\": \"authorName541\"\n}"
    //    val jsonObject = JSON.parseObject(aa);
    val t = JSON.parseObject(aa, classOf[Td2])

    print(t)
//    val a: Option[Integer] = t.id
    //    println("a:"+a.toString)
//    println("aa" + a.isDefined)
//    val a: Integer = t.id match {
//      case Some(v) => v.intValue()
//      case _ => -1
//    }
//    println("ddd" + a)
//    println("aa" + getValueOrElse(t.id))
    val aaa = t.haha

    get(aaa)
  }
  def get(m:Integer): Unit ={
    println("dsfd:"+m)
  }

  def getValueOrElse(num: Option[Integer]): Integer = {
    val a: Integer = num match {
      case Some(v) => v.intValue()
      case _ => -1
    }
    println("ddd" + a)
    //    if (num == null ) {
    //      println("ulll")
    //    } else if (num == None){
    //      println("None")
    //
    //    }else{
    //      println("what")
    //    }
    return num.getOrElse(-1)
  }
}

//case class Tdl(id: Integer,title: String,atableNameuthor: String,op:String,tsMs:Long,alsoLike9Url:String)
case class Tdl(id: Integer=1, title: String, author: String)

case class Td2(id: Option[Integer], title: String, authorName: String, haha: Int)
package kafka.schema_registery

import scala.util.parsing.json.JSON

/*
*Author : Prudhvi Akella.
*Description : This Program is used to transfer the AVRO Schema file from Local to Schema Registry
 */

import scalaj.http._

object a_RegisterSchema {
  def main(args:Array[String]): Unit ={
    //It Requires three Arguments
    //1: Schema  Registery URL
    //2: Topic Name
    //3: Schema File path
    //4: Schema For Key or value
    /*For passing program arguments go to run-->Edit configurations-->Enter program arguments
    * http://localhost:8081 <topicname> <schema path> <key or value>*/
    if(args.length != 4){
      println("Usage: a_RegisterSchema <Schema Registery URL> <Topic Name> <Schema File Path> <Key/Value>")
      return
    }
    val schema_registery_host_url = args(0)
    val topic_name = args(1)
    val Schema_file_path = args(2).toString()
    val keyorValue = args(3)
    //Preparing URL its pretty much looks like //http:localhost:8081/subjects/order-value/versions
    val URL = schema_registery_host_url + "/subjects/" + topic_name + "-"+keyorValue+"/versions"
    val json_file_schema = scala.io.Source.fromFile(Schema_file_path).mkString.replace("\"", "\\\"").replace("\n","").replaceAll(" +", "")
    //Preparing MutliLine JSON String and interpolating json_file_schema variable(String Interpolation)
    val json_content =
      s"""
        |{
        |"schema":"${json_file_schema}"
        |}
        |""".stripMargin.toString
    if(json_file_schema != null) {
      //preparing post request to schema registery
      val request: HttpRequest = Http(URL).header("Content-Type", "application/vnd.schemaregistry.v1+json").postData(json_content)
      println(request.toString)
      val response = request.asString.body
      print(response)
    }
  }
}

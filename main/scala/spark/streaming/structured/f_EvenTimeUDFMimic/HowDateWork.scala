package spark.streaming.structured.f_EvenTimeUDFMimic

import java.text.SimpleDateFormat
import java.util.Calendar

object HowDateWork extends App {
  def add_timestamp():String= {
    val now = Calendar.getInstance().getTime()
    val date = new SimpleDateFormat("Y-m-d H:M:s")
    println(date.format(now))
    date.format(now).toString
  }

  println(add_timestamp())
}

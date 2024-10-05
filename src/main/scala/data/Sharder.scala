package data

object Sharder {
  def shardText(text: String, numShards: Int): Array[String] = {
    val words = text.split("\\s+")
    val shardSize = (words.length.toDouble / numShards).ceil.toInt
    words.grouped(shardSize).map(_.mkString(" ")).toArray
  }

}

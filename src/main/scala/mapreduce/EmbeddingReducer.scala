package mapreduce

import embedding.Encoder
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.nd4j.linalg.factory.Nd4j
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import java.io.IOException
import scala.collection.mutable.ListBuffer



class EmbeddingReducer extends Reducer[Text, Text, Text, Text] {
  private val logger = LoggerFactory.getLogger(classOf[EmbeddingReducer])
  val encoder = new Encoder
  // Accumulate tokens from the shard
  private val collectedEmbeddings = ListBuffer[String]()

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val embeddingVectors = values.asScala.map { value =>
      val vectorArray = value.toString.split(",").map(_.toDouble)
      Nd4j.create(vectorArray)
    }.toList

    // Compute the average of these vectors
    val sumVector = embeddingVectors.reduce(_ add _)
    val averageVector = sumVector.div(embeddingVectors.size)

    // Convert the average vector to a string representation
    val tokenID = key.toString.toInt
    val tokenWord = try {
      // Attempt to decode the token, handle missing tokens gracefully
      encoder.decode(Seq(tokenID))
    } catch {
      case e: NoSuchElementException =>
        logger.warn(s"Token ID $tokenID not found in encoder. Using 'UNKNOWN'.")
        "UNKNOWN" // Default to "UNKNOWN" if token is not found
    }

    val embeddingStr = averageVector.toDoubleVector.mkString(",")

    collectedEmbeddings += s"$tokenID,$tokenWord,$embeddingStr"

    context.write(key, new Text(averageVector.toDoubleVector.mkString(",")))
  }
//
//  // Cleanup method in Reducer
//  override def cleanup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
//    logger.info("Reducer task finished. Cleanup called.")
//    val embeddingCsv = new Path("/user/hadoop/output/embeddings.csv")
//    val fs = embeddingCsv.getFileSystem(context.getConfiguration)
//    val outputStream = fs.create(embeddingCsv, true)
//
//    collectedEmbeddings.foreach(token => {
//      outputStream.writeBytes(s"""$token
//""")
//    })

//    outputStream.close()

}


//class EmbeddingReducer extends Reducer[Text, Text, Text, Text] {
//
//  override def reduce(word: Text, embeddings: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
//    val embedding = embeddings.iterator().next() // Since there's only one embedding per word
//    context.write(word, embedding)
//  }
//}

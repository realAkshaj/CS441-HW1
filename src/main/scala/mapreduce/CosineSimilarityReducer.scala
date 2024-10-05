package mapreduce

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.*

class CosineSimilarityReducer extends Reducer[Text, Text, Text, Text] {

  private val logger = LoggerFactory.getLogger(classOf[CosineSimilarityReducer])

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val similarities = values.asScala.mkString(" ")
    logger.info(s"Reducing for word '${key.toString}'")
    context.write(key, new Text(similarities))
  }
}





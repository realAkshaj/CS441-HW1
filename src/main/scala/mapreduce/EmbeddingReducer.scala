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


import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

class EmbeddingReducer extends Reducer[Text, Text, Text, Text] {
  class EmbeddingReducer extends Reducer[Text, Text, Text, Text] {
    private val logger = LoggerFactory.getLogger(classOf[EmbeddingReducer])
    private var multipleOutputs: MultipleOutputs[Text, Text] = _

    override def setup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      multipleOutputs = new MultipleOutputs[Text, Text](context)
    }

    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val token = key.toString

      values.asScala.foreach { embeddingText =>
        // Write to the regular output
        context.write(new Text(token), embeddingText)

        // Additionally, write to a custom file called "all_embeddings"
        multipleOutputs.write("embeddings_output", key, embeddingText)
      }

      logger.debug(s"Emitted embedding for token $token: ${values.asScala.mkString}")
    }

    override def cleanup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      multipleOutputs.close()
    }
  }
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



//class EmbeddingReducer extends Reducer[Text, Text, Text, Text] {
//
//  override def reduce(word: Text, embeddings: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
//    val embedding = embeddings.iterator().next() // Since there's only one embedding per word
//    context.write(word, embedding)
//  }
//}

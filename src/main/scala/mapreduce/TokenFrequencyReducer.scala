package mapreduce

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.io.{BufferedWriter, FileWriter, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*


class TokenFrequencyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

  private var tokensWriter: BufferedWriter = _

  override def setup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    // Get the output path from the job context
    val outputPath = FileOutputFormat.getOutputPath(context)

    // Define the HDFS path for the tokens_output_only.txt file
    val tokensOutputPath = new Path(outputPath, "tokens_output_only.txt")

    // Get the Hadoop FileSystem instance
    val fs = FileSystem.get(context.getConfiguration)

    // Create the BufferedWriter for writing the file to HDFS
    tokensWriter = new BufferedWriter(new OutputStreamWriter(fs.create(tokensOutputPath, true), StandardCharsets.UTF_8))
  }

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    // Sum the values (using Scala's asScala to convert to an iterable)
    val sum = values.asScala.map(_.get).sum

    // Write the token (key) to the tokens_output_only.txt file on HDFS
    tokensWriter.write(key.toString)
    tokensWriter.newLine()

    // Optionally, write key and sum to context as part of the output
    context.write(key, new IntWritable(sum))
  }

  override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    // Close the BufferedWriter
    if (tokensWriter != null) {
      tokensWriter.close()
    }
  }
}



//class TokenFrequencyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
//  private val result = new IntWritable()
//
//  @throws[IOException]
//  @throws[InterruptedException]
//  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
//    val sum = values.asScala.map(_.get).sum // Sum the frequencies for each word-token pair
//    result.set(sum)
//    context.write(key, result) // Output word-token pair and frequency
//  }
//}



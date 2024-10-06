package mapreduce

import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import java.io.{BufferedWriter, FileWriter, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*


class TokenFrequencyReducer extends Reducer[Text, IntWritable, Text, Text] {

  var tokensFile: BufferedWriter = _

  // Override the setup method to initialize the tokens file based on the task attempt ID
  @throws[IOException]
  @throws[InterruptedException]
  override def setup(context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    val taskId = context.getTaskAttemptID.getTaskID.getId
    val tokensFilePath = Paths.get(s"tokens_only_$taskId.txt")
    tokensFile = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(tokensFilePath), StandardCharsets.UTF_8))
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    val sum = values.asScala.map(_.get).sum
    val parts = key.toString.split(",")

    if (parts.length == 2) {  // Ensure that the key has both word and token
      val word = parts(0).trim
      val token = parts(1).trim

      // Write the final output as word - token - frequency
      context.write(new Text(s"$word - $token - $sum"), new Text(""))

      // Write the token to the tokens-only file
      tokensFile.write(s"$token\n")
    } else {
      println(s"Warning: Invalid key format in reducer: $key")
    }
  }

  // Close the tokens file in the cleanup phase
  override def cleanup(context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    if (tokensFile != null) {
      tokensFile.close()
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



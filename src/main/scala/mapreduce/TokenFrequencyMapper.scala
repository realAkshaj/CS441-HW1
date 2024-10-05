package mapreduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import tokenizer.Tokenizer

import java.io.{BufferedWriter, File, FileWriter, IOException, OutputStreamWriter}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import java.util.UUID
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import tokenizer.Tokenizer

import java.nio.charset.StandardCharsets

class TokenFrequencyMapper extends Mapper[Object, Text, Text, IntWritable] {

  val wordWithTokens = new Text()
  val one = new IntWritable(1)

  // BufferedWriter for writing tokens to a local file
  var hdfsWriter: BufferedWriter = _

  @throws[IOException]
  @throws[InterruptedException]
  override def setup(context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    // Ensure the output directory exists
    val conf = context.getConfiguration
    val fs = FileSystem.get(conf)
    val hdfsPath = new Path("hdfs:///output/tokens_output_only.txt")
    
    if (fs.exists(hdfsPath)) {
      fs.delete(hdfsPath,true) // Create the directory if it doesn't exist
    }

    val hdfsOutputStream = fs.create(hdfsPath, true)
    hdfsWriter = new BufferedWriter(new OutputStreamWriter(hdfsOutputStream, StandardCharsets.UTF_8))
  }


  @throws[IOException]
  @throws[InterruptedException]
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val words = value.toString.split("\\s+").map(_.trim).filter(_.nonEmpty)

    words.foreach { word =>
      // Tokenize the word
      val tokens = Tokenizer.tokenize(word)

      // Emit word-token pair to the context for MapReduce processing
      wordWithTokens.set(s"$word , ${tokens.mkString(",")},")
      context.write(wordWithTokens, one)

      // Write the tokens to the local file in resources/output/tokens_output_only.txt
      synchronized {
        hdfsWriter.write(s"${tokens.mkString(",")}\n")
      }
    }
  }

  @throws[IOException]
  override def cleanup(context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    // Close the token file writer
    if (hdfsWriter != null) {
      hdfsWriter.close()
    }
  }
}

//class TokenFrequencyMapper extends Mapper[Object, Text, Text, IntWritable] {
//
//  val wordWithTokens = new Text()
//  val one = new IntWritable(1)
//
//  @throws[IOException]
//  @throws[InterruptedException]
//  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
//    val words = value.toString.split("\\s+").map(_.trim).filter(_.nonEmpty)
//
//    words.foreach { word =>
//      val tokens = Tokenizer.tokenize(word).mkString(", ") // Tokenize the word
//      wordWithTokens.set(s"$word - ${tokens.mkString(",")} -") // Set the word and tokens as the key
//      context.write(wordWithTokens, one) // Emit word-token pair with frequency 1
//    }
//  }
//}

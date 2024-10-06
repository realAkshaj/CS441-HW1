package mapreduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import tokenizer.Tokenizer
import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, FileWriter, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable


class TokenFrequencyMapper extends Mapper[Object, Text, Text, IntWritable] {

  val wordWithTokens = new Text()
  val one = new IntWritable(1)
  var tokensFile: BufferedWriter = _  // File for writing tokens only

  // Override the setup method to initialize the tokens file
  @throws[IOException]
  override def setup(context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val tokensFilePath: Path = Paths.get("resources/output/onlytokens.txt")

    // Ensure the output directory exists, if not create it
    val parentDir = tokensFilePath.getParent
    if (!Files.exists(parentDir)) {
      Files.createDirectories(parentDir)
    }

    // Open the file for writing
    tokensFile = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(tokensFilePath), StandardCharsets.UTF_8))
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val words = value.toString.split("\\s+").map(_.trim).filter(_.nonEmpty)

    // Tokenize each word and write the tokens both to the context and the separate file
    words.foreach { word =>
      val tokens = Tokenizer.tokenize(word)
      tokens.foreach { token =>
        // Emit word-token pair as key and 1 as IntWritable value for frequency counting
        wordWithTokens.set(s"$word,$token")
        context.write(wordWithTokens, one)

        // Also, write the token to the tokens-only file
        tokensFile.write(s"$token\n")
      }
    }
  }

  // Override cleanup method to close the tokens file
  @throws[IOException]
  override def cleanup(context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    if (tokensFile != null) {
      tokensFile.close()
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

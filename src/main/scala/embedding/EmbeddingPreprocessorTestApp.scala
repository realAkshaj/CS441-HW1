//package embedding
//
//import org.nd4j.linalg.api.ndarray.INDArray
//import scala.io.Source
//
//object EmbeddingPreprocessorTestApp {
//
//  def readTokensFromFile(filePath: String): Seq[Array[Int]] = {
//    val source = Source.fromFile(filePath)
//    try {
//      // Split the lines and map to arrays of integers, handling both single and comma-separated tokens
//      source.getLines().map { line =>
//        line.split(",").map(_.toInt)
//      }.toSeq
//    } finally {
//      source.close()
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    // Sample token IDs
//    val tokenFilePath = "resources/output/tokens_output_only.txt"
//    val sampleTokens: Seq[Int] = readTokensFromFile(tokenFilePath).flatten
//
//
//    // Set windowSize to 2 and stride to 1
//    val windowSize: Int = 1
//    val stride: Int = 1
//
//    // Generate embeddings using the EmbeddingPreprocessor
//    val embeddingsMap: Map[Int, INDArray] = EmbeddingPreprocessor.generateEmbeddingsForTokens(sampleTokens, windowSize, stride)
//
//    // Print the embeddings for each token
//    println("\nGenerated Embeddings:")
//    embeddingsMap.foreach { case (tokenID, embedding) =>
//      println(s"Token ID: $tokenID, Embedding: ${embedding.toDoubleVector.mkString(", ")}")
//    }
//  }
//}
//
//
//
//

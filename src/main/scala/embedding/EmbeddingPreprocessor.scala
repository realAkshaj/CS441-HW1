package embedding

import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.api.buffer.DataType
import org.deeplearning4j.nn.conf.{MultiLayerConfiguration, NeuralNetConfiguration}
import org.deeplearning4j.nn.conf.layers.{EmbeddingSequenceLayer, OutputLayer}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import utils.CustomReshapeUtil


import java.io.{File, FileWriter, PrintWriter}

object EmbeddingPreprocessor {
  val encoder = new Encoder

  def generateEmbeddingsForTokens(encodedTokens: Seq[Int], windowSize: Int, stride: Int): Map[Int, INDArray] = {
    // Step 1: Remap the tokens
    val (remappedDecoded, tokenToIndex) = remapTokens(encodedTokens)

    // Step 2: Prepare input-output pairs and convert to INDArrays
    val inputOutputPairs = encoder.createInputOutputPairs(remappedDecoded, windowSize, stride)
    val (inputFeatures, outputLabels) = convertToIndArrays(inputOutputPairs, tokenToIndex.size)

    // Step 3: Dynamically set vocabSize based on the number of unique tokens
    val vocabSize = tokenToIndex.size
    val embeddingDim = 50

    // Step 4: Configure the neural network
    val config: MultiLayerConfiguration = new NeuralNetConfiguration.Builder()
      .weightInit(WeightInit.XAVIER)
      .list(
        // Embedding Layer
        new EmbeddingSequenceLayer.Builder()
          .nIn(vocabSize)
          .nOut(embeddingDim)
          .build(),

        // Output Layer
        new OutputLayer.Builder(LossFunction.SPARSE_MCXENT)
          .nIn(embeddingDim * windowSize)
          .nOut(vocabSize)
          .activation(Activation.SOFTMAX)
          .build()
      )
      .inputPreProcessor(1, new CustomReshapeUtil(windowSize, embeddingDim)) // Preprocess before layer 1
      .build()

    val model = new MultiLayerNetwork(config)
    model.init()
    model.setListeners(new ScoreIterationListener(100))

    // Step 5: Train the model
    val numEpochs = 10  // Reduce number of epochs for faster test runs
    for (epoch <- 1 to numEpochs) {
      model.fit(inputFeatures, outputLabels)
      println(s"Completed epoch $epoch")
    }

    // Step 6: Extract embeddings from the model
    val embeddings: INDArray = model.getLayer(0).getParam("W")
    val indexToToken = tokenToIndex.map(_.swap) // Reverse the map to get index -> token

    // Step 7: Build embedding map with error handling for missing tokens
    val embeddingsMap: Map[Int, INDArray] = indexToToken.flatMap { case (index, tokenId) =>
      try {
        val embeddingVector = embeddings.getRow(index).dup() // Get embedding for the token
        Some(tokenId -> embeddingVector)
      } catch {
        case e: Exception =>
          println(s"Error: Token ID $tokenId not found in embedding matrix.")
          None
      }
    }

    // Return the map containing each token ID mapped to its embedding vector
    embeddingsMap
  }

  def remapTokens(decodedTokens: Seq[Int]): (Seq[Int], Map[Int, Int]) = {
    val uniqueTokens = decodedTokens.distinct.sorted
    val tokenToIndex = uniqueTokens.zipWithIndex.toMap
    val remappedTokens = decodedTokens.map(tokenToIndex)
    (remappedTokens, tokenToIndex)
  }

  def convertToIndArrays(inputOutputPairs: Seq[(Array[Int], Int)], vocabSize: Int): (INDArray, INDArray) = {
    val inputSequences: Array[Array[Double]] = inputOutputPairs.map { case (inputArray, _) =>
      inputArray.map(_.toDouble)
    }.toArray

    // Convert target tokens to Array[Double]
    val targetTokens: Array[Double] = inputOutputPairs.map { case (_, target) =>
      target.toDouble
    }.toArray

    // Create INDArrays from the arrays
    val inputFeatures: INDArray = Nd4j.create(inputSequences).castTo(DataType.UINT32)
    val outputLabels: INDArray = Nd4j.create(targetTokens).reshape(-1, 1).castTo(DataType.UINT32)

    (inputFeatures, outputLabels)
  }
}
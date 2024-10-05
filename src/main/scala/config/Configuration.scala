package config

import com.typesafe.config.{Config, ConfigFactory}

object Configuration {
  private val config: Config = ConfigFactory.load()

  val applicationName: String = config.getString("app.name")
  val trainingText: String = config.getString("app.input_train")
  val testingText: String = config.getString("app.input_test")
  val sampleText: String = config.getString("app.input_sample")
  val validationText: String = config.getString("app.input_validate")
  val numShards: Int = config.getInt("app.num_shards")
  val vocabularySize: Int = config.getInt("app.vocabulary_size")
  val embeddingDimensions: Int = config.getInt("app.embedding_dimensions")
  
  
  

}

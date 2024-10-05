package data
import config.Configuration.*

object DataCleaner {
  def cleanText(text: String): String = {
    text.replaceAll("[^a-zA-Z\\s]", "").toLowerCase
  }
}

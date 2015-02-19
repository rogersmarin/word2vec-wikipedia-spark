package com.rogersmarin.word2vec

/**
 * Created by rogerm on 19/02/2015.
 */

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.Word2Vec

import com.rogersmarin.word2vec.WikiParser._

object RunWord2Vec {

  val WikiDumpFile = "/data/wikipedia-dump/18022015/enwiki-latest-pages-articles.xml"
  val StopwordsFile = "src/main/resources/stopwords.txt"

  def main (args: Array[String]) {
    val sampleSize = 0.1
    val conf = new SparkConf().setAppName("Word2Vec Wikipedia (en)")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val pages = readFile(WikiDumpFile, sc).sample(false, sampleSize, 11L)

    val plainText = pages.filter(_ != null).flatMap(wikiXmlToPlainText)

    val stopWords = sc.broadcast(loadStopWords(StopwordsFile)).value

    val lemmatized = plainText.mapPartitions(iter => {
      val pipeline = createNLPPipeline()
      iter.map{ case(title, contents) => (title, plainTextToLemmas(contents, stopWords, pipeline))}
    })

    val filtered = lemmatized.filter(_._2.size > 1).values.collect()

    val filteredRDD = sc.parallelize(filtered)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(filteredRDD)

    val synonyms = model.findSynonyms("Germany", 40)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

  }

}

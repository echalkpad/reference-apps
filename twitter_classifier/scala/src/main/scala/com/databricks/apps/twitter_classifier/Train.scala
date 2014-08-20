package com.databricks.apps.twitter_classifier

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Train {
  val jsonParser = new JsonParser()
  val gson = new GsonBuilder().setPrettyPrinting().create()

  def main(args: Array[String]) {
    // Process program arguments and set properties
    if (args.length < 3) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <tweetDirectory> <numClusters> <numIterations>")
      System.exit(1)
    }
    val Array(tweetDirectory, Utils.IntParam(numClusters), Utils.IntParam(numIterations)) = args

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    // TODO(vida): Can we take out transient?
    @transient val sqlContext = new SQLContext(sc)

    // Pretty print some of the tweets.
    val tweets = sc.textFile(tweetDirectory)
    println("------------Sample JSON Tweets-------")
    for (tweet <- tweets.take(5)) {
      println(gson.toJson(jsonParser.parse(tweet)))
    }
    println("---------End Sample JSON Tweets--------")


    val tweetTable = sqlContext.jsonFile(tweetDirectory)
    println("------Tweet table Schema---")
    tweetTable.printSchema
    println("------End Tweet table Schema---")

    tweetTable.registerTempTable("tweetTable")

    println("----Tweet Text-----")
    sqlContext.sql("SELECT text FROM tweetTable LIMIT 10").collect.foreach(println)
    println("------End Tweet Text---")

    println("------Lang, Name, text---")
    sqlContext.sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 10").collect.foreach(println)
    println("------End Lang, Name, text---")

    println("------Lang, count(*)---")
    sqlContext.sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 10").collect.foreach(println)
    println("------End Lang, count(*)---")

    println("About to grab tweet text for training")
    val rows = sqlContext.sql("SELECT text from tweetTable")
    val texts = rows.map(_.head.toString)
    val vectors = texts.map(Utils.featurize).cache()
    vectors.count()

    val model = KMeans.train(vectors, numClusters, numIterations)

    val some_tweets = texts.take(100)

    for (i <- 0 until numClusters) {
      println(s"\nCLUSTER $i:")
      some_tweets.foreach { t =>
        if (model.predict(Utils.featurize(t)) == i) {
          println(t)
        }
      }
    }

    sc.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile("%s/%s".format(tweetDirectory, "model"))
  }
}

package com.databricks.apps.twitter_classifier

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import twitter4j.auth.{OAuthAuthorization, Authorization}
import twitter4j.conf.ConfigurationBuilder

object Utils {
  // SECRET AUTH DETAILS BELOW... SHHHHHH
  def getAuth(): Option[Authorization] = {
    System.setProperty("twitter4j.oauth.consumerKey", "VykrQpfuXnBGIS2YsbtmQ")
    System.setProperty("twitter4j.oauth.consumerSecret", "lM6vM9zWc5LMU2839XkRYpaU3wKqWkxEjT5M6yDxsQ")
    System.setProperty("twitter4j.oauth.accessToken", "62295096-JVlI10q7tLR8hLYinkFzbXRD0XG5SsLd7rnSCFsL0")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "mIXeSpe0sDEhsgRdWdeho5zn4xdL5xEKv2MwzOCd4")
    Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  }

  /**
   * Create feature vectors by turning each tweet into bigrams of characters (an n-gram model)
   * and then hashing those to a length-1000 feature vector that we can pass to MLlib.
   * This is a common way to decrease the number of features in a model while still
   * getting excellent accuracy (otherwise every pair of Unicode characters would
   * potentially be a feature).
   */
  def featurize(s: String): Vector = {
    val n = 1000
    val result = new Array[Double](n)
    val bigrams = s.sliding(2).toArray
    for (h <- bigrams.map(_.hashCode % n)) {
      result(h) += 1.0 / bigrams.length
    }
    Vectors.sparse(n, result.zipWithIndex.filter(_._1 != 0).map(_.swap))
  }

  object IntParam {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }
  }
}

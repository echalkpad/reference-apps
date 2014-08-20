# Twitter Streaming Language Classifier

In this reference application, we show how you can use Apache Spark for training a language classifier - replacing a whole suite of tools you may be currently using.

This reference application was demoed at a meetup which is taped here - the link skips straight to demo time, but the talk before that is useful too:

[![IMAGE ALT TEXT HERE](http://img.youtube.com/vi/FjhRkfAuU7I/0.jpg)](https://www.youtube.com/watch?v=FjhRkfAuU7I#t=2030)

These are the 5 typical stages for creating a production ready classifer - oftentimes each stage is done with a different set of tools and even by different engineering teams:

1. Scrape/collect a dataset.
* Clean and explore the data, doing feature extraction.
* Build a model on the data and iterate/improve it.
* Improve the model using more and more data, perhaps upgrading your infrastructure to support building larger models.  (Such as migrating over to Hadoop.)
* Apply the model in real time.

Instead, this reference application only has two simple Spark programs to do Twitter Streaming Language Classification.

1. With one simple Spark program, we collect the data, clean it, and train a model on it.  This one program will do all the following:
  * Collect a dataset with Spark Streaming, cleaning the data as we go along.  In our case, we'll be collecting Tweets as our data.
  * Build a model on the data. We'll use MLLib to cluster the tweets.
  * Depending on the size of the dataset you've collected - you can train a small model on your local machine - or you can train the model on a bigger Spark cluster.  Scaling comes for free when you use Spark.
* Apply the model in real time.  We'll use Spark Streaming to filter live Tweets coming in for those that are similiar to a cluster of tweets we've selected.


# BigDataAssignment3
Spark and spark streaming

Your task is to work on a streaming data source, i.e. a real-time stream ofnews articles coming from various sources. For each article, a title, description, and category label is provided.

Using Spark, your task for this assignment is as follows

Construct a historical data set using the data stream. Get started with this as soon as possible
 - Make sure to set up Spark first using the instructions posted above if you haven't done so already
 - You can use the provided examples in that package to get started
 - The streaming server is running at seppe.net:7778 (note that the examples use port 7777, which streams reddit articles, here, you'll have to use 7778)
Construct a predictive model to predict the categorization based on the title anddescription (multi-classification): offline training
Use your model to predict some new titles in a streaming setup 
The third part of your lab report should contain:

The source code of your programs, as well as the output after running them
Use to provided guide to get tips and hints on how to approach the given problems
Feel free to include screen shots or info on encountered challenges and how you dealt with them
Even if your solution is not fully working or not working correctly, you can still receive marks for this assignment if you show what you tried and how you'd need to improve your end result (i.e. when you can prove a conceptual understanding of the problem and solution)
Further remarks:

Get started with setting up Spark and fetching articles as quickly as possible. IF you encounter troubles getting Spark to run, do let me know
Note that the articles will stop coming in after awhile (the backend feed is queried every 15 minutes or so, as news doesn’t appear every minute)
The instances themselves are line delimited and will be easy to convert to an RDD entry. The text itself splits the title, description and category using a tab character ("\t"). Perform some exploratory investigation on your data set to figure out the amount of categories, word composition, length, and so on
Python is recommended, but R can be used as well (see below for instructions to get started)
The higher your accuracy, the better, though the focus of this assignment is on getting the full pipeline constructed, so no need for deep neural nets or word embeddings, though TF-IDF, stemming, stop word removal might be interesting to apply. Additionally, some categories might be easier to predict than others (sports, for instance, has some typical key terms)
You can use Spark Streaming (most likely easier) or Spark Structured Streaming
Your predictive model needs to be build using Mllib (read documentation and tutorials). In case you really encounter trouble, you can use scikit-learn as well to still perform the deployment stage. Any type or approach is valid
If you'd like to bring in other packages to help out with the preprocessing of the text, that's fine too (not required though you can), though the predictive model itself should be a Spark one
Let me know in case the streaming server would crash… simply with an e-mail

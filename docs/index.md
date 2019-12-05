Siddhi Execution Streaming ML
======================================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-streamingml/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-streamingml/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-execution-streamingml.svg)](https://github.com/siddhi-io/siddhi-execution-streamingml/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-execution-streamingml.svg)](https://github.com/siddhi-io/siddhi-execution-streamingml/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-execution-streamingml.svg)](https://github.com/siddhi-io/siddhi-execution-streamingml/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-execution-streamingml.svg)](https://github.com/siddhi-io/siddhi-execution-streamingml/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-execution-streamingml extension** is a <a target="_blank" href="https://siddhi.io/">Siddhi</a> extension that provides streaming machine learning (clustering, classification and regression) on event streams.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 2.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.execution.streamingml/siddhi-execution-streamingml/">here</a>.
* Versions 1.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.execution.streamingml/siddhi-execution-streamingml">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-streamingml/api/2.0.4">2.0.4</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-streamingml/api/2.0.4/#bayesianregression-stream-processor">bayesianRegression</a> *(<a target="_blank" href="http://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This extension predicts using a Bayesian linear regression model.Bayesian linear regression allows determining the uncertainty of each prediction by estimating the full-predictive distribution</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-streamingml/api/2.0.4/#kmeansincremental-stream-processor">kMeansIncremental</a> *(<a target="_blank" href="http://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>Performs K-Means clustering on a streaming data set. Data points can be of any dimension and the dimensionality is calculated from number of parameters. All data points to be processed by a query should be of the same dimensionality. The Euclidean distance is taken as the distance metric. The algorithm resembles Sequential K-Means Clustering at https://www.cs.princeton.edu/courses/archive/fall08/cos436/Duda/C/sk_means.htm </p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-streamingml/api/2.0.4/#kmeansminibatch-stream-processor">kMeansMiniBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>Performs K-Means clustering on a streaming data set. Data points can be of any dimension and the dimensionality is calculated from number of parameters. All data points to be processed in a single query should be of the same dimensionality. The Euclidean distance is taken as the distance metric. The algorithm resembles mini-batch K-Means. (refer Web-Scale K-Means Clustering by D.Sculley, Google, Inc.). </p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-streamingml/api/2.0.4/#perceptronclassifier-stream-processor">perceptronClassifier</a> *(<a target="_blank" href="http://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This extension predicts using a linear binary classification Perceptron model.</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-streamingml/api/2.0.4/#updatebayesianregression-stream-processor">updateBayesianRegression</a> *(<a target="_blank" href="http://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This extension builds/updates a linear Bayesian regression model. This extension uses an improved version of stochastic variational inference.</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-streamingml/api/2.0.4/#updateperceptronclassifier-stream-processor">updatePerceptronClassifier</a> *(<a target="_blank" href="http://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This extension builds/updates a linear binary classification Perceptron model.</p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-streamingml/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

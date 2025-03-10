# API Docs - v2.0.6

!!! Info "Tested Siddhi Core version: *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/">5.1.21</a>*"
    It could also support other Siddhi Core minor versions.

## Streamingml

### bayesianRegression *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This extension predicts using a Bayesian linear regression model.Bayesian linear regression allows determining the uncertainty of each prediction by estimating the full-predictive distribution</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
streamingml:bayesianRegression(<STRING> model.name, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:bayesianRegression(<STRING> model.name, <INT> prediction.samples, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">model.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the model to be used</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">prediction.samples</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The number of samples to be drawn to estimate the prediction</p></td>
        <td style="vertical-align: top">1000</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.feature</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The features of the model that need to be attributes of the stream</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT<br>INT<br>LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">prediction</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The predicted value (double)</p></td>
        <td style="vertical-align: top">DOUBLE</td>
    </tr>
    <tr>
        <td style="vertical-align: top">confidence</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Inverse of the standard deviation of the predictive distribution</p></td>
        <td style="vertical-align: top">DOUBLE</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double);

from StreamA#streamingml:bayesianRegression('model1', attribute_0, attribute_1, attribute_2, attribute_3) 
insert all events into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query uses a Bayesian regression model named <code>model1</code> to predict the label of the feature vector represented by <code>attribute_0</code>, <code>attribute_1</code>, <code>attribute_2</code>, and <code>attribute_3</code>. The predicted value is emitted to the <code>OutputStream</code> streamalong with the prediction confidence (std of predictive distribution) and the feature vector. As a result, the OutputStream stream is defined as follows: (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, prediction double, confidence double).</p>
<p></p>
### kMeansIncremental *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Performs K-Means clustering on a streaming data set. Data points can be of any dimension and the dimensionality is calculated from number of parameters. All data points to be processed by a query should be of the same dimensionality. The Euclidean distance is taken as the distance metric. The algorithm resembles Sequential K-Means Clustering at https://www.cs.princeton.edu/courses/archive/fall08/cos436/Duda/C/sk_means.htm </p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
streamingml:kMeansIncremental(<INT> no.of.clusters, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:kMeansIncremental(<INT> no.of.clusters, <DOUBLE> decay.rate, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">no.of.clusters</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The assumed number of natural clusters in the data set.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">decay.rate</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">this is the decay rate of old data compared to new data. Value of this will be in [0,1]. 0 means only old data used and1 will mean that only new data is used</p></td>
        <td style="vertical-align: top">0.01</td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.feature</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is a variable length argument. Depending on the dimensionality of data points we will receive coordinates as features along each axis.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT<br>INT<br>LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">euclideanDistanceToClosestCentroid</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Represents the Euclidean distance between the current data point and the closest centroid.</p></td>
        <td style="vertical-align: top">DOUBLE</td>
    </tr>
    <tr>
        <td style="vertical-align: top">closestCentroidCoordinate</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is a variable length attribute. Depending on the dimensionality(D) we will return closestCentroidCoordinate1, closestCentroidCoordinate2,... closestCentroidCoordinateD which are the d dimensional coordinates of the closest centroid from the model to the current event. This is the prediction result and this represents the cluster to which the current event belongs to.</p></td>
        <td style="vertical-align: top">DOUBLE</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream InputStream (x double, y double);
@info(name = 'query1')
from InputStream#streamingml:kMeansIncremental(2, 0.2, x, y)
select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This is an example where user provides the decay rate. First two events will be used to initiate the model since the required number of clusters is specified as 2. After the first event itself prediction would start.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream InputStream (x double, y double);
@info(name = 'query1')
from InputStream#streamingml:kMeansIncremental(2, x, y)
select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This is an example where user doesnt give the decay rate so the default value will be used</p>
<p></p>
### kMeansMiniBatch *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Performs K-Means clustering on a streaming data set. Data points can be of any dimension and the dimensionality is calculated from number of parameters. All data points to be processed in a single query should be of the same dimensionality. The Euclidean distance is taken as the distance metric. The algorithm resembles mini-batch K-Means. (refer Web-Scale K-Means Clustering by D.Sculley, Google, Inc.). </p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
streamingml:kMeansMiniBatch(<INT> no.of.clusters, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:kMeansMiniBatch(<INT> no.of.clusters, <DOUBLE> decay.rate, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:kMeansMiniBatch(<INT> no.of.clusters, <INT> maximum.iterations, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:kMeansMiniBatch(<INT> no.of.clusters, <INT> no.of.events.to.retrain, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:kMeansMiniBatch(<INT> no.of.clusters, <DOUBLE> decay.rate, <INT> maximum.iterations, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:kMeansMiniBatch(<INT> no.of.clusters, <DOUBLE> decay.rate, <INT> no.of.events.to.retrain, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:kMeansMiniBatch(<INT> no.of.clusters, <INT> maximum.iterations, <INT> no.of.events.to.retrain, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:kMeansMiniBatch(<INT> no.of.clusters, <DOUBLE> decay.rate, <INT> maximum.iterations, <INT> no.of.events.to.retrain, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">no.of.clusters</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The assumed number of natural clusters in the data set.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">decay.rate</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">this is the decay rate of old data compared to new data. Value of this will be in [0,1]. 0 means only old data used and1 will mean that only new data is used</p></td>
        <td style="vertical-align: top">0.01</td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">maximum.iterations</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Number of iterations, the process iterates until the number of maximum iterations is reached or the centroids do not change</p></td>
        <td style="vertical-align: top">50</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">no.of.events.to.retrain</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">number of events to recalculate cluster centers. </p></td>
        <td style="vertical-align: top">20</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.feature</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is a variable length argument. Depending on the dimensionality of data points we will receive coordinates as features along each axis.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT<br>INT<br>LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">euclideanDistanceToClosestCentroid</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Represents the Euclidean distance between the current data point and the closest centroid.</p></td>
        <td style="vertical-align: top">DOUBLE</td>
    </tr>
    <tr>
        <td style="vertical-align: top">closestCentroidCoordinate</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is a variable length attribute. Depending on the dimensionality(d) we will return closestCentroidCoordinate1 to closestCentroidCoordinated which are the d dimensional coordinates of the closest centroid from the model to the current event. This is the prediction result and this represents the cluster towhich the current event belongs to.</p></td>
        <td style="vertical-align: top">DOUBLE</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream InputStream (x double, y double);
@info(name = 'query1')
from InputStream#streamingml:kMeansMiniBatch(2, 0.2, 10, 20, x, y)
select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This is an example where user gives all three hyper parameters. first 20 events will be consumed to build the model and from the 21st event prediction would start</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream InputStream (x double, y double);
@info(name = 'query1')
from InputStream#streamingml:kMeansMiniBatch(2, x, y)
select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This is an example where user has not specified hyper params. So default values will be used.</p>
<p></p>
### perceptronClassifier *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This extension predicts using a linear binary classification Perceptron model.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
streamingml:perceptronClassifier(<STRING> model.name, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:perceptronClassifier(<STRING> model.name, <DOUBLE> model.bias, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:perceptronClassifier(<STRING> model.name, <DOUBLE> model.threshold, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:perceptronClassifier(<STRING> model.name, <DOUBLE> model.bias, <DOUBLE> model.threshold, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">model.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the model to be used.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.bias</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The bias of the Perceptron algorithm.</p></td>
        <td style="vertical-align: top">0.0</td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.threshold</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The threshold that separates the two classes. The value specified must be between zero and one.</p></td>
        <td style="vertical-align: top">0.5</td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.feature</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The features of the model that need to be attributes of the stream.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT<br>INT<br>LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">prediction</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The predicted value (<code>true/false</code>)</p></td>
        <td style="vertical-align: top">BOOL</td>
    </tr>
    <tr>
        <td style="vertical-align: top">confidenceLevel</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The probability of the prediction</p></td>
        <td style="vertical-align: top">DOUBLE</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double);

from StreamA#streamingml:perceptronClassifier('model1',0.0,0.5, attribute_0, attribute_1, attribute_2, attribute_3) 
insert all events into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query uses a Perceptron model named <code>model1</code> with a <code>0.0</code> bias and a <code>0.5</code> threshold learning rate to predict the label of the feature vector represented by <code>attribute_0</code>, <code>attribute_1</code>, <code>attribute_2</code>, and <code>attribute_3</code>. The predicted label (<code>true/false</code>) is emitted to the <code>OutputStream</code> streamalong with the prediction confidence level(probability) and the feature vector. As a result, the OutputStream stream is defined as follows: (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, prediction bool, confidenceLevel double).</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double);

from StreamA#streamingml:perceptronClassifier('model1',0.0, attribute_0, attribute_1, attribute_2, attribute_3) 
insert all events into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query uses a Perceptron model named <code>model1</code> with a <code>0.0</code> bias to predict the label of the feature vector represented by <code>attribute_0</code>, <code>attribute_1</code>, <code>attribute_2</code>, and <code>attribute_3</code>. The prediction(<code>true/false</code>) is emitted to the <code>OutputStream</code>stream along with the prediction confidence level(probability) and the feature. As a result, the OutputStream stream is defined as follows: (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, prediction bool, confidenceLevel double).</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double);

from StreamA#streamingml:perceptronClassifier(`model1`, attribute_0, attribute_1, attribute_2) 
insert all events into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query uses a Perceptron model named <code>model1</code> with a default 0.0 bias to predict the label of the feature vector represented by <code>attribute_0</code>, <code>attribute_1</code>, and <code>attribute_2</code>. The predicted probability is emitted to the OutputStream stream along with the feature vector. As a result, the OutputStream is defined as follows: (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, prediction bool, confidenceLevel double).</p>
<p></p>
### updateBayesianRegression *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This extension builds/updates a linear Bayesian regression model. This extension uses an improved version of stochastic variational inference.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
streamingml:updateBayesianRegression(<STRING> model.name, <INT|DOUBLE|LONG|FLOAT> model.target, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:updateBayesianRegression(<STRING> model.name, <INT|DOUBLE|LONG|FLOAT> model.target, <INT> model.samples, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:updateBayesianRegression(<STRING> model.name, <INT|DOUBLE|LONG|FLOAT> model.target, <STRING> model.optimizer, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:updateBayesianRegression(<STRING> model.name, <INT|DOUBLE|LONG|FLOAT> model.target, <DOUBLE> learning.rate, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:updateBayesianRegression(<STRING> model.name, <INT|DOUBLE|LONG|FLOAT> model.target, <INT> model.samples, <STRING> model.optimizer, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:updateBayesianRegression(<STRING> model.name, <INT|DOUBLE|LONG|FLOAT> model.target, <INT> model.samples, <DOUBLE> learning.rate, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:updateBayesianRegression(<STRING> model.name, <INT|DOUBLE|LONG|FLOAT> model.target, <STRING> model.optimizer, <DOUBLE> learning.rate, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:updateBayesianRegression(<STRING> model.name, <INT|DOUBLE|LONG|FLOAT> model.target, <INT> model.samples, <STRING> model.optimizer, <DOUBLE> learning.rate, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">model.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the model to be built.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.target</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The target attribute (dependant variable) of the input stream.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>DOUBLE<br>LONG<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.samples</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Number of samples used to construct the gradients.</p></td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.optimizer</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The type of optimization used</p></td>
        <td style="vertical-align: top">ADAM</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">learning.rate</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The learning rate of the updater</p></td>
        <td style="vertical-align: top">0.05</td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.feature</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Features of the model that need to be attributes of the stream.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT<br>INT<br>LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">loss</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;"> loss of the model.</p></td>
        <td style="vertical-align: top">DOUBLE</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, attribute_4 double );

from StreamA#streamingml:updateBayesianRegression('model1', attribute_4, attribute_0, attribute_1, attribute_2, attribute_3) 
insert all events into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query builds/updates a Bayesian Linear regression model named <code>model1</code> using <code>attribute_0</code>, <code>attribute_1</code>, <code>attribute_2</code>, and <code>attribute_3</code> as features, and <code>attribute_4</code> as the label. Updated weights of the model are emitted to the OutputStream stream.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, attribute_4 double );

from StreamA#streamingml:updateBayesianRegression('model1', attribute_4, 2, 'NADAM', 0.01, attribute_0, attribute_1, attribute_2, attribute_3) 
insert all events into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query builds/updates a Bayesian Linear regression model named <code>model1</code> with a <code>0.01</code> learning rate using <code>attribute_0</code>, <code>attribute_1</code>, <code>attribute_2</code>, and <code>attribute_3</code> as features, and <code>attribute_4</code> as the label. Updated weights of the model are emitted to the OutputStream stream. This model draws two samples during monte-carlo integration and uses NADAM optimizer.</p>
<p></p>
### updatePerceptronClassifier *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This extension builds/updates a linear binary classification Perceptron model.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
streamingml:updatePerceptronClassifier(<STRING> model.name, <BOOL|STRING> model.label, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
streamingml:updatePerceptronClassifier(<STRING> model.name, <BOOL|STRING> model.label, <DOUBLE> learning.rate, <DOUBLE|FLOAT|INT|LONG> model.feature, <DOUBLE|FLOAT|INT|LONG> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">model.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the model to be built/updated.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.label</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The attribute of the label or the class of the dataset.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">BOOL<br>STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">learning.rate</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The learning rate of the Perceptron algorithm.</p></td>
        <td style="vertical-align: top">0.1</td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">model.feature</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Features of the model that need to be attributes of the stream.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT<br>INT<br>LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">featureWeight</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Weight of the &lt;feature.name&gt; of the model.</p></td>
        <td style="vertical-align: top">DOUBLE</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, attribute_4 string );

from StreamA#streamingml:updatePerceptronClassifier('model1', attribute_4, 0.01, attribute_0, attribute_1, attribute_2, attribute_3) 
insert all events into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query builds/updates a Perceptron model named <code>model1</code> with a <code>0.01</code> learning rate using <code>attribute_0</code>, <code>attribute_1</code>, <code>attribute_2</code>, and <code>attribute_3</code> as features, and <code>attribute_4</code> as the label. Updated weights of the model are emitted to the OutputStream stream.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double,attribute_3 double, attribute_4 string );

 from StreamA#streamingml:updatePerceptronClassifier('model1', attribute_4, attribute_0, attribute_1, attribute_2, attribute_3) 
insert all events into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query builds/updates a Perceptron model named <code>model1</code> with a default <code>0.1</code> learning rate using <code>attribute_0</code>, <code>attribute_1</code>, <code>attribute_2</code>, and <code>attribute_3</code> as features, and <code>attribute_4</code> as the label. The updated weights of the model are appended to the outputStream.</p>
<p></p>

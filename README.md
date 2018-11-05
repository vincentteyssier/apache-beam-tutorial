
# Hands on Apache Beam, building data pipelines in Python


Apache Beam is an open-source SDK which allows you to build multiple data pipelines from batch or stream based integrations and run it in a direct or distributed way. You can add various transformations in each pipeline. But the real power of Beam comes from the fact that it is not based on a specific compute engine and therefore is platform independant. You declare which 'runner' you want to use to compute your transformation. It is using your local computing resource by default, but you can specify a Spark engine for example or Cloud DataflowΓÇª

In this article, I will create a pipeline ingesting a csv file, computing the mean of the Open and Close columns fo a historical S&P500 dataset. The goal here is not to give an extensive tutorial on Beam features, but rather to give you an overall idea of what you can do with it and if it is worth for you going deeper in building custom pipelines with Beam. Though I only write about batch processing, streaming pipelines are a powerful feature of Beam!

Beam's SDK can be used in various languages, Java, Python... however in this article I will focus on Python.

![](https://cdn-images-1.medium.com/max/2000/1*kBQv5-3eva_tgz2qZ0oICg.png)

## Installation

At the date of this article Apache Beam (2.8.1) is only compatible with Python 2.7, however a Python 3 version should be available soon. If you have python-snappy installed, Beam may crash. This issue is known and will be fixed in Beam 2.9.

    pip install apache-beam

## Creating a basic pipeline ingesting CSV

## Data

For this example we will use a csv containing historical values of the S&P 500. The data looks like that:

    Date,Open,High,Low,Close,Volume
    03-01-00,1469.25,1478,1438.359985,1455.219971,931800000
    04-01-00,1455.219971,1455.219971,1397.430054,1399.420044,1009000000

## Basic pipeline

To create a pipeline, we need to instantiate the pipeline object, eventually pass some options, and declaring the steps/transforms of the pipeline.

    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions

    options = PipelineOptions()
    p = beam.Pipeline(options=options)

From the beam documentation:
> Use the pipeline options to configure different aspects of your pipeline, such as the pipeline runner that will execute your pipeline and any runner-specific configuration required by the chosen runner. Your pipeline options will potentially include information such as your project ID or a location for storing files.

The PipelineOptions() method above is a command line parser that will read any standard option passed the following way:

    --<option>=<value>

## Custom options

You can also build your custom options. In this example I set an input and an output folder for my pipeline:

    **class** **MyOptions**(PipelineOptions):

    @classmethod
      **def** **_add_argparse_args**(cls, parser):
        parser**.**add_argument('--input',
                            help**=**'Input for the pipeline',
                            default**=**'./data/')
        parser**.**add_argument('--output',
                            help**=**'Output for the pipeline',
                            default**=**'./output/')

## Transforms principles

In Beam, data is represented as a ***PCollection ***object. So to start ingesting data, we need to read from the csv and store this as a ***PCollection*** to which we can then apply transformations. The Read operation is considered as a transform and follows the syntax of all transformations:

    [Output PCollection] **=** [Input PCollection] **|** [Transform]

These tranforms can then be chained like this:

[Final Output PCollection] **=** ([Initial Input PCollection] **|** [First Transform]
 **|** [Second Transform]
 **|** [Third Transform])

The pipe is the equivalent of an *apply* method.

The input and output PCollections, as well as each intermediate PCollection are to be considered as individual data containers. This allows to apply multiple transformations to the same PCollection as the initial PCollection is immutable. For example:

    [Output PCollection 1] **=** [Input PCollection] **|** [Transform 1]
    [Output PCollection 2] **=** [Input PCollection] **|** [Transform 2]

## Reading input data and writing output data

So let's start by using one of the readers provided to read our csv, not forgetting to skip the header row:

    csv_lines = (p | ReadFromText(input_filename, skip_header_lines=1) |   ...

At the other end of our pipeline we want to output a text file. So let"s use the standard writer:

    ... **|** beam**.**io**.**WriteToText(output_filename)

## Transforms

Now we want to apply some transformations to our PCollection created with the Reader function. Transforms are applied to each element of the PCollection individually.

Depending on the worker that you chose, your transforms can be distributed. Instances of your transformation are then executed on each node.
> The user code running on each worker generates the output elements that are ultimately added to the final output *PCollection* that the transform produces.

Beam has core methods (ParDo, Combine) that allows to apply a custom transform , but also has pre written transforms called [composite transforms](https://beam.apache.org/documentation/programming-guide/#composite-transforms). In our example we will use the ParDo transform to apply our own functions.

We have read our csv into a ***PCollection***, so let's split it so we can access the Date and Close items:

    ΓÇª beam.ParDo(Split()) ΓÇª

And define our split function so we only retain the Date and Close and return it as a dictionnary:

    class Split(beam.DoFn):
        def process(self, element):
            Date,Open,High,Low,Close,Volume = element.split(ΓÇ£,ΓÇ¥)
            return [{
                "Open": float(Open),
                "Close": float(Close),
            }]

Now that we have the data we need, we can use one of the [standard combiners](https://beam.apache.org/releases/pydoc/2.6.0/_modules/apache_beam/transforms/combiners.html#Mean) to calculate the mean over the entire PCollection.

The first thing to do is to represent the data as a tuple so we can group by a key and then feed CombineValues with what it expects. To do that we use a custom function "CollectOpen()" which returns a list of tuples containing (1, <open_value>).

    class CollectOpen(beam.DoFn):
        def process(self, element):
            # Returns a list of tuples containing Date and Open value
            result = [(1, element["Open"])]
            return result

The first parameter of the tuple is fixed since we want to calculate the mean over the whole dataset, but you can make it dynamic to perform the next transform only on a sub-set defined by that key.

The GroupByKey function allows to create a PCollection of all elements for which the key (ie the left side of the tuples) is the same.

    mean_open = (
        csv_lines | beam.ParDo(CollectOpen()) |
        "Grouping keys Open" >> beam.GroupByKey() |
        "Calculating mean for Open" >> beam.CombineValues(
            beam.combiners.MeanCombineFn()
        )
    )

When you assign a label to a transform, make sure it is unique, otherwise Beam will throw an error.

Our final pipeline could look like that if we want to chain everything:

    csv_lines = (
        p | beam.io.ReadFromText(input_filename) | 
        beam.ParDo(Split()) |
        beam.ParDo(CollectOpen()) |
        "Grouping keys Open" >> beam.GroupByKey() |
        "Calculating mean" >> beam.CombineValues(
            beam.combiners.MeanCombineFn()
        ) | beam**.**io**.**WriteToText(output_filename)
    )

But we could also write it in a way that allows to add future transformation on the splitted PCollection (like a mean of the close for example):

    csv_lines = (
        p | beam.io.ReadFromText(input_filename) |
        beam.ParDo(Split())
    )

    mean_open = (
        csv_lines | beam.ParDo(CollectOpen()) |
        "Grouping keys Open" >> beam.GroupByKey() |
        "Calculating mean for Open" >> beam.CombineValues(
            beam.combiners.MeanCombineFn()
        )
    )

    output = (
        mean_open | beam**.**io**.**WriteToText(output_filename)
    ) 

## Multiple transforms on the same PCollection

If I want to add another transform operation on the csv_lines PCollection I will obtain a second ΓÇ£transformed PCollectionΓÇ¥. Beam represents it very well in the form of ΓÇ£branchedΓÇ¥ tranformations:

![](https://cdn-images-1.medium.com/max/2000/1*RRxNdNFG-e9t6w_EO2cPgA.png)

To apply the different transforms we would have :

    csv_lines = (
        p | beam.io.ReadFromText(input_filename) |
        beam.ParDo(Split())
    )

    mean_open = (
        csv_lines | beam.ParDo(CollectOpen()) |
        "Grouping keys Open" >> beam.GroupByKey() |
        "Calculating mean for Open" >> beam.CombineValues(
            beam.combiners.MeanCombineFn()
        )
    )

    mean_close = (
        csv_lines | beam.ParDo(CollectClose()) |
        "Grouping keys Close" >> beam.GroupByKey() |
        "Calculating mean for Close" >> beam.CombineValues(
            beam.combiners.MeanCombineFn()
        )
    )

But now we have 2 PCollections: mean_open and mean_close, as a result of the transform. We need to merge/join these results to get a PCollection we could write on a file with our writer. Beam has the CoGroupByKeywhich is doing just that. Our output would then look like that:

    output= ( 
        { 
            "Mean Open": mean_open,
            "Mean Close": mean_close
        } | 
        apache_beam.CoGroupByKey() | 
        WriteToText(output_filename))
    )

We have now our pipeline defined end to end. You can run it by command line using the custom arguments we have defined earlier:

    python test_beam.py **--**input ./data/sp500.csv **--**output ./output/result.txt 

The final result in the file looks like that:

    (1, {"Mean Close": [1482.764536822227], "Mean Open": [1482.5682959997862]})

## Beam Readers and Writers

In this example we only used the csv reader and text writer, but Beam has much more connectors (ufortunately most of them are available for the Java platform, but a few Python ones are in progress). You can find the list of available connectors and their documentation at:
[**Built-in I/O Transforms**
*Apache Beam is an open source, unified model and set of language-specific SDKs for defining and executing dataΓÇª*beam.apache.org](https://beam.apache.org/documentation/io/built-in/)

You can also find a guide to write your own connectors if you feel brave enough:
[**Authoring I/O Transforms - Overview**
*Apache Beam is an open source, unified model and set of language-specific SDKs for defining and executing dataΓÇª*beam.apache.org](https://beam.apache.org/documentation/io/authoring-overview/)

## General Logic when creating data pipelines

Whenever a data pipeline needs to be implemented, we want to be clear on the requirements and the end goal of our pipeline/transformations. In Beam documentation I found this little extract which I think is the core of how you should reason when starting to build a pipeline with Beam:
> **Where is your input data stored?** How many sets of input data do you have? This will determine what kinds of Read transforms you"ll need to apply at the start of your pipeline.
> **What does your data look like?** It might be plaintext, formatted log files, or rows in a database table. Some Beam transforms work exclusively on PCollections of key/value pairs; you"ll need to determine if and how your data is keyed and how to best represent that in your pipeline"s PCollection(s).
> **What do you want to do with your data?** The core transforms in the Beam SDKs are general purpose. Knowing how you need to change or manipulate your data will determine how you build core transforms like [ParDo](https://beam.apache.org/documentation/programming-guide/#pardo), or when you use pre-written transforms included with the Beam SDKs.
> **What does your output data look like, and where should it go?** This will determine what kinds of Write transforms you"ll need to apply at the end of your pipeline.

## Using a distributed runner

As said earlier, instead of using the local compute power (DirectRunner) you can use a distributed compute engine such as Spark. You can do that by setting the following options to your pipeline options (in command line arguments or in an option list):

    --runner SparkRunner  --sparkMaster spark://host:port

More options are available [here](https://beam.apache.org/documentation/runners/spark/), but these 2 are the basics.

## Conclusion

Beam is quite low level when it comes to write custom transformation, then offering the flexibily one might need. It is fast and handles cloud / distributed environments. If you look at a higher level API/SDK, some libraries like tf.transform are actually built on top of Beam and offer you its power while coding less. The trade-off lays in the flexibility you are looking for.

The code for this article is available on GitHub [here](https://github.com/vincentteyssier/apache-beam-tutorial).


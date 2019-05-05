# Just a stupid Apache Beam demo...

* Clone this repo
* `mvn compile`
* `mvn exec:java -Dexec.mainClass=com.ridge.demo.StarterPipeline -Dexec.args="--runner=DirectRunner --project=[GCP_PROJECT]"`
* Pump some JSON messages through pub/sub and see what happens!

The pipeline frst filters out any non-JSON messages, and creates collections of any that have a `type` of `event` and those that don't. This creates a PCollectionTuple<String> for the two collections. The two collections in the  PCollectionTuple are then processed to log each string, using two pipelines.

This demonstrates splitting one input PCollection, and processing each split collection.

Note - this example uses the identity used to run the GCloud CLI for authentication, although in your environment you could use an environment variable and a JSON key-file to change that without touching this code.

HTH!
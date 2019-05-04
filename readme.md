# Just a stupid Apache Beam demo...

* Clone this repo
* `mvn compile`
* `mvn exec:java -Dexec.mainClass=com.ridge.demo.StarterPipeline -Dexec.args="--runner=DirectRunner --projec[GCP_PROJECT]"`
* Pump some JSON messages through pub/sub and see what happens!

The pipeline frst filters out any non-JSON messages, and any that don't have a type of `event`. This creates a PCollection<String> based on the `meta` property of the message. This PCollection is then processed to log each string.

Note - this example uses the identity used to run the GCloud CLI for authentication, although in your environment you could use an environment variable and a JSON key-file to change that without touching this code.

HTH!
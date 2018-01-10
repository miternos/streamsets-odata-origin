# streamsets-signalr-origin

Streamsets origin example for Odata interface

How to use
==========
- Build the project with maven
- Install the fat jar (-with-dependencies) into stream sets as an external jar
- Restart StreamSets and you are ready to use custom components in pipeline

How it works
============
Gets all the SensorMeasures from ODATA interface, puts to a cache. Then servers these into the pipeline. Periodically the data is re-fetched and sent to the pipeline again.



package com.thekensta.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created 20/06/2017.
 */
public class PartitionExample {



    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String [] args) {





        ExampleOptions exampleOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(ExampleOptions.class);

        String inputSource = exampleOptions.getInputSource();
        String outputFolder = exampleOptions.getOutputFolder();

        String eventList = exampleOptions.getEventList();
        String [] eventNames = eventList.split(",");


        /*
         * Build the events to identify from the command line
         */
        Map<String, Integer> eventToIndex = new HashMap<>();
        eventToIndex.put("error", 0);
        eventToIndex.put("unknown", 1);

        int i = 2;
        for (String e: eventNames) {
            eventToIndex.put(e, i);
            i += 1;
        }

        Map<Integer, String> indexToEvent = new HashMap<>();
        for (String key : eventToIndex.keySet()) {
            indexToEvent.put(eventToIndex.get(key), key);
        }

        Pipeline pipeline = Pipeline.create(exampleOptions);

        PCollection<String> inputText = pipeline.apply("Read Text Input",
                TextIO.read().from(inputSource));

        PCollectionList<String> partitionedEvents = inputText.apply(
                "Partition By Event",
                Partition.of(eventToIndex.size(), new Partition.PartitionFn<String>() {
                    public int partitionFor(String s, int i) {
                        try {
                            JsonNode node = mapper.readTree(s);
                            String event = node.get("event").asText();
                            return eventToIndex.getOrDefault(event, 1);
                        } catch (IOException e) {
                            return 0;
                        }
                    }
                }));


        for (int index = 0; index < partitionedEvents.size(); index++) {

            PCollection<String> events = partitionedEvents.get(index);
            String sink = indexToEvent.get(index);
            events.apply("Write [" + sink + "]",
                    TextIO.write().to(outputFolder + "/" + sink + "/data")
                            .withSuffix(".json"));
        }

        pipeline.run().waitUntilFinish();

    }

}

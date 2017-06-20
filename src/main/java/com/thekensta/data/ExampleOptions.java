package com.thekensta.data;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Created 20/06/2017.
 */
public interface ExampleOptions extends PipelineOptions {

    @Description("Input Source")
    @Default.String("/tmp/events.ndjson")
    String getInputSource();
    void setInputSource(String inputSource);

    @Description("Output Folder")
    @Default.String("/tmp")
    String getOutputFolder();
    void setOutputFolder(String outputFolder);

    @Description("Comma delimited events to match")
    @Default.String("search,purchase")
    String getEventList();
    void setEventList(String eventList);

}

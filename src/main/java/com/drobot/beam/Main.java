package com.drobot.beam;

import com.drobot.beam.pipeline.AvroToBigQueryWriter;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

public class Main {

    public static void main(String[] args) {
        Pipeline pipeline = AvroToBigQueryWriter.createPipeline(args);
        PipelineResult pipelineResult = AvroToBigQueryWriter.run(pipeline);
        if (pipeline.getOptions().as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
            pipelineResult.waitUntilFinish();
        }
    }
}

package com.demo.batch;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import com.demo.batch.config.BatchPipelineOptions;

public class Application {
	public static void main(String[] args) {
		BatchPipelineOptions pipelineOptions = PipelineOptionsFactory
				.fromArgs(args)
				.as(BatchPipelineOptions.class);
		
		Pipeline p = Pipeline.create(pipelineOptions);
		
		BatchPipeline.build(p).run();
	}
}

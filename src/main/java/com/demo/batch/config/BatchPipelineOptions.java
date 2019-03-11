package com.demo.batch.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.ValueProvider;

public interface BatchPipelineOptions extends DataflowPipelineOptions{

	@Default.String("")
	ValueProvider<String> getSubwayDelayFilePrefix();
	void setSubwayDelayFilePrefix(ValueProvider<String> subwayDelayFilePrefix);
	
	@Default.String("")
	ValueProvider<String> getSubwayLogCodes();
	void setSubwayLogCodes(ValueProvider<String> subwayLogCodes);

	@Default.String("")
	ValueProvider<String> getStreetcarDelayFilePrefix();
	void setStreetcarDelayFilePrefix(ValueProvider<String> streetcarDelayFilePrefix);
	
	@Default.String("")
	ValueProvider<String> getBusDelayFilePrefix();
	void setBusDelayFilePrefix(ValueProvider<String> busDelayFilePrefix);
	
	@Default.String("")
	ValueProvider<String> getTableSpec();
	void setTableSpec(ValueProvider<String> tableSpec);

	@Default.String("gs://scotia-demo-resouce/BigQueryTempLocation")
	ValueProvider<String> getCustomGcsTempLocation();
	void setCustomGcsTempLocation(ValueProvider<String> customGcsTempLocation);
}

package com.demo.batch.dofn;

import java.text.SimpleDateFormat;

import org.apache.beam.sdk.transforms.DoFn;
import com.demo.batch.model.TransitDelay;
import com.google.api.services.bigquery.model.TableRow;

public class ParseToTableRow extends DoFn<TransitDelay, TableRow> {

	@ProcessElement
	public void processElement (ProcessContext c) {
		TransitDelay input = c.element();
		c.output(new TableRow()
				.set("transitType", input.getType().name())
				.set("date", new SimpleDateFormat("yyyy-MM-dd").format(input.getDate()))
				.set("time", input.getTime())
				.set("day", input.getDay())
				.set("transitRoute", input.getTransitRoute())
				.set("transitLocation", input.getTransitLocation())
				.set("reasonForDelay", input.getReasonForDelay())
				.set("delayAmount", input.getDelayAmount())
				.set("gapAmount", input.getGapAmount())
				.set("direction", input.getDirection())
				.set("vehicleNumber", input.getVehicleNumber()));	
	}
}

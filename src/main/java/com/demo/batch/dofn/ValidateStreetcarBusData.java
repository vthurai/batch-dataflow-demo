package com.demo.batch.dofn;

import java.sql.Date;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.beam.repackaged.beam_sdks_java_core.com.google.common.base.Strings;
import org.apache.beam.sdk.transforms.DoFn;
import com.demo.batch.model.TransitDelay;
import com.demo.batch.model.TransitDelay.TransitType;

/*
 * Field Name	Description											Example
 * Report Date	The date (YYYY/MM/DD) when the delay-causing		6/20/2017
 * 				incident occurred	
 * Route		The number of the streetcar route					51
 * Time			The time (hh:mm:ss AM/PM) when the delay-			12:35:00 AM
 * 				causing incident occurred	
 * Day			The name of the day									Monday
 * Location		The location of the delay-causing incident			York Mills Station
 * Incident		The description of the delay-causing incident		Mechanical
 * Min Delay	The delay, in minutes, to the schedule for the		10
 * 				following streetcar	
 * Min Gap		The total scheduled time, in minutes, from the		20
 * 				streetcar ahead of the following streetcar			
 * Direction	"The direction of the bus route where B,b or BW 	EB - eastbound,
 * 				indicates both ways. (On an east west route, it		SB - southbound,
 * 				includes both east and west)                        WB - westbound ,                   
 * 				Note: The direction is not case sensitive"			NB - northbound
 * Vehicle		Vehicle number										1057

 */

public class ValidateStreetcarBusData extends DoFn<String, TransitDelay> {

	private static final long serialVersionUID = 6550614477209929295L;

	private Logger logger;
	private final int NUM_OF_COLS = 10;
	private TransitType transitType;
	public ValidateStreetcarBusData(TransitType transitType) {
		this.transitType = transitType;
	}
	
	@Setup
	public void setup() {
		logger = Logger.getLogger(ValidateSubwayData.class.getName());
	}
	
	@ProcessElement
	public void processElement (ProcessContext c) {

		LinkedList<String> row = new LinkedList<String>(Arrays.asList(c.element().split(",")));
		try {
			String vehicleNumber = (row.size() == NUM_OF_COLS) ? row.get(9) : "MISSING"; 
			TransitDelay transitDelay = new TransitDelay(transitType);
			transitDelay.setDate(Date.valueOf(row.get(0)));
			transitDelay.setTransitRoute(row.get(1));
			transitDelay.setTime(Time.valueOf(row.get(2)));
			transitDelay.setDay(row.get(3));
			transitDelay.setTransitLocation(row.get(4));
			transitDelay.setReasonForDelay(row.get(5));
			transitDelay.setDelayAmount(Integer.parseInt(row.get(6)));
			transitDelay.setGapAmount(Integer.parseInt(row.get(7)));
			transitDelay.setDirection(row.get(8));
			transitDelay.setVehicleNumber(vehicleNumber);
		
			logger.info(transitDelay.toString());
			c.output(transitDelay);
		} catch(Exception e) {
			logger.log(Level.SEVERE, "String " + c.element() 
			+ " contains either an incorrect code or missing data in one column", e);
		}
	}

}


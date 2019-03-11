package com.demo.batch.dofn;

import java.sql.Date;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import com.demo.batch.model.TransitDelay;
import com.demo.batch.model.TransitDelay.TransitType;
import com.google.common.base.Charsets;

import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/*
 * Subway Delay Metadata:
 * 
 * Col.	Field Name	Description									Example
 * 0	Date		Date (YYYY/MM/DD)*							12/31/2016
 * 1	Time		Time (24h clock)							1:59
 * 2	Day			Name of the day of the week					Saturday
 * 3	Station		TTC subway station name						Rosedale Station
 * 4	Code**		TTC delay code								MUIS
 * 5	Min Delay	Delay (in minutes) to subway service		5
 * 6	Min Gap		Time length (in minutes) between trains		9
 * 7	Bound		Direction of train dependant on the line	N
 * 8	Line		TTC subway line i.e. YU, BD, SHP, and SRT	YU
 * 9	Vehicle		TTC train number							5961
 * 
 * Note:
 *  1. Date format actually in MM/DD/YYYY
 *  2. Code needs to be mapped to the incident string representation
 */

public class ValidateSubwayData extends DoFn<String, TransitDelay> {

	private static final long serialVersionUID = 6550614477209929295L;

	private PCollectionView<Map<String, String>> subwayCodeMapping;
	private Logger logger;
	
	public ValidateSubwayData(PCollectionView<Map<String, String>> subwayCodeMapping) {
		this.subwayCodeMapping = subwayCodeMapping;
	}
	
	@Setup
	public void setup() {
		logger = Logger.getLogger(ValidateSubwayData.class.getName());
	}
	
	@ProcessElement
	public void processElement (ProcessContext c) {
		Map<String, String> codeMapping = c.sideInput(subwayCodeMapping);
		LinkedList<String> row = new LinkedList<String>(Arrays.asList(c.element().split(",")));
		try {
			String incident = (codeMapping.containsKey(row.get(4))) ? codeMapping.get(row.get(4)) : "Code is missing";
			TransitDelay transitDelay = new TransitDelay(TransitType.SUBWAY);
			transitDelay.setDate(new SimpleDateFormat("yyyy/MM/dd").parse(row.get(0)));
			transitDelay.setTime(Time.valueOf(new StringBuilder(row.get(1)).append(":00").toString()));
			transitDelay.setDay(row.get(2));
			transitDelay.setTransitLocation(row.get(3));
			transitDelay.setReasonForDelay(incident);
			transitDelay.setDelayAmount(Integer.parseInt(row.get(5)));
			transitDelay.setGapAmount(Integer.parseInt(row.get(6)));
			transitDelay.setDirection(returnDirection(row.get(7)));
			transitDelay.setTransitRoute(returnSubwayLine(row.get(8)));
			transitDelay.setVehicleNumber(row.get(9));
		
			logger.info(transitDelay.toString());
			c.output(transitDelay);
		} catch(ParseException e) {
			logger.log(Level.WARNING, "String " + c.element() + " is in a invalid format", e);
		} catch(Exception e) {
			logger.log(Level.SEVERE, "String " + c.element() 
			+ " contains either an incorrect code or missing data in one column", e);
		}
	}
	
	public String returnSubwayLine(String code) throws ParseException {
		switch(code.toUpperCase()) {
			case "YU":
				return "Yonge-University Line";
			case "BD":
				return "Bloor-Danforth Line";
			case "SRT":
				return "Scarborough RT Line";
			case "SHP":
				return "Sheppard Line";
			default:
				throw new ParseException(code + " is not in the correct format", 1);
		}
	}
	
	public String returnDirection(String code) {
		switch(code.toUpperCase()) {
			case "N":
				return "Northbound";
			case "S":
				return "Southbound";
			case "E":
				return "Eastbound";
			case "W":
				return "Westbound";
			default:
				return "MISSING";
		}		
	}
}

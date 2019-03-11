package com.demo.batch.model;

import java.sql.Time;
import java.util.Date;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

@DefaultCoder(AvroCoder.class)
public class TransitDelay {
	
	private TransitType type;
	private Date date;
	private Date time;
	private String day;
	private String transitRoute;
	private String transitLocation;
	private String reasonForDelay;
	private int delayAmount;
	private int gapAmount;
	private String direction;
	private String vehicleNumber;
	
	public static enum TransitType {
		SUBWAY, STREETCAR, BUS
	}
	
	public TransitDelay(TransitType type) {
		this.type = type;
	}
	
	public TransitDelay(TransitType type, Date date, Date time, String day, String transitRoute, String transitLocation,
			String reasonForDelay, int delayAmount, int gapAmount, String direction, String vehicleNumber) {
		this.type = type;
		this.date = date;
		this.time = time;
		this.day = day;
		this.transitRoute = transitRoute;
		this.transitLocation = transitLocation;
		this.reasonForDelay = reasonForDelay;
		this.delayAmount = delayAmount;
		this.gapAmount = gapAmount;
		this.direction = direction;
		this.vehicleNumber = vehicleNumber;
	}
	
	public TransitType getType() {
		return type;
	}

	public void setType(TransitType type) {
		this.type = type;
	}


	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public String getDay() {
		return day;
	}
	public void setDay(String day) {
		this.day = day;
	}
	public String getTransitRoute() {
		return transitRoute;
	}
	public void setTransitRoute(String transitRoute) {
		this.transitRoute = transitRoute;
	}
	public String getTransitLocation() {
		return transitLocation;
	}
	public void setTransitLocation(String transitLocation) {
		this.transitLocation = transitLocation;
	}
	public String getReasonForDelay() {
		return reasonForDelay;
	}
	public void setReasonForDelay(String reasonForDelay) {
		this.reasonForDelay = reasonForDelay;
	}
	public int getDelayAmount() {
		return delayAmount;
	}
	public void setDelayAmount(int delayAmount) {
		this.delayAmount = delayAmount;
	}
	public int getGapAmount() {
		return gapAmount;
	}
	public void setGapAmount(int gapAmount) {
		this.gapAmount = gapAmount;
	}
	public String getDirection() {
		return direction;
	}
	public void setDirection(String direction) {
		this.direction = direction;
	}
	public String getVehicleNumber() {
		return vehicleNumber;
	}
	public void setVehicleNumber(String vehicleNumber) {
		this.vehicleNumber = vehicleNumber;
	}
	
	@Override
	public String toString() {
		return "TransitDelay [type=" + type + ", " + "date=" + date + ", " + "time=" + time + ", day=" + day + ", transitRoute=" + transitRoute
				+ ", transitLocation=" + transitLocation + ", reasonForDelay=" + reasonForDelay + ", delayAmount="
				+ delayAmount + ", gapAmount=" + gapAmount + ", direction=" + direction + ", vehicleNumber="
				+ vehicleNumber + "]";
	}
	
	public static TableSchema getTransitDelaySchema() {
		return new TableSchema().setFields(
				ImmutableList.of(
		           new TableFieldSchema().setName("transitType").setType("STRING").setMode("NULLABLE"),//REQUIRED
		           new TableFieldSchema().setName("date").setType("DATE").setMode("NULLABLE"),
		           new TableFieldSchema().setName("time").setType("TIME").setMode("NULLABLE"),
		           new TableFieldSchema().setName("day").setType("STRING").setMode("NULLABLE"),
		           new TableFieldSchema().setName("transitRoute").setType("STRING").setMode("NULLABLE"),
		           new TableFieldSchema().setName("transitLocation").setType("STRING").setMode("NULLABLE"),
		           new TableFieldSchema().setName("reasonForDelay").setType("STRING").setMode("NULLABLE"),						     
		           new TableFieldSchema().setName("delayAmount").setType("INTEGER").setMode("NULLABLE"),
		           new TableFieldSchema().setName("gapAmount").setType("INTEGER").setMode("NULLABLE"),
		           new TableFieldSchema().setName("direction").setType("STRING").setMode("NULLABLE"),
		           new TableFieldSchema().setName("vehicleNumber").setType("STRING").setMode("NULLABLE")
		));
	}
}

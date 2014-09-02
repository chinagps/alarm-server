package com.gps.alarm.placeserver;

import java.util.HashMap;

public class Alarm extends HashMap<String, Object> {

	private static final long serialVersionUID = 2014081417573212345L;

	private long id;
	
	private double x;
	
	private double y;
	
	private String place;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}
	
	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public String getPlace() {
		return place;
	}

	public void setPlace(String place) {
		this.place = place;
	}
}

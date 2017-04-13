package edu.ruc.entity;

import java.io.Serializable;

/**
 * Created by Tao on 2017/4/13.
 */
public class Weather implements Serializable {
    private String location;
    private int month;
    private int dayofyear;
    private int year;
    private float temperature;

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDayofyear() {
        return dayofyear;
    }

    public void setDayofyear(int dayofyear) {
        this.dayofyear = dayofyear;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    public Weather() {
    }

    public Weather(String location, int month, int dayofyear, int year, float temperature) {
        this.location = location;
        this.month = month;
        this.dayofyear = dayofyear;
        this.year = year;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "Weather{" +
                "location='" + location + '\'' +
                ", month=" + month +
                ", dayofyear=" + dayofyear +
                ", year=" + year +
                ", temperature=" + temperature +
                '}';
    }
}

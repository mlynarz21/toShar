import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

public class Weather {
//    public static final HashMap<String, Weather> weatherDict = loadWeatherDict("/user/cloudera/in/weather/2016Weather.csv");
//    public static final HashMap<String, Weather> weatherDict = loadWeatherDict("/home/cloudera/Desktop/2016Weather.csv");
    private LocalDateTime dateTime;
    private String conditions;
    private boolean isFog;
    private boolean isRain;
    private boolean isSnow;

    public Weather(LocalDateTime dateTime, String conditions, boolean isFog, boolean isRain, boolean isSnow) {
        this.dateTime = dateTime;
        this.conditions = conditions;
        this.isFog = isFog;
        this.isRain = isRain;
        this.isSnow = isSnow;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public String getConditions() {
        return conditions;
    }

    public boolean isFog() {
        return isFog;
    }

    public boolean isRain() {
        return isRain;
    }

    public boolean isSnow() {
        return isSnow;
    }


}
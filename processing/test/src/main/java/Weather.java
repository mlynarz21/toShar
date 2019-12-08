import java.time.LocalDateTime;

public class Weather {
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
}

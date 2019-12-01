import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Test {

    public static void main(String[] args) throws IOException{
//        String link = getLink("40.780", "-73.967");
//        String link2 = getLink("40.865", "-73.859");
//        System.out.println(getDistrict(link));
//        System.out.println(getDistrict(link2));
        System.out.println(loadWeatherDict("C:\\Users\\mlyna\\Desktop\\2016Weather.csv").size());
//        List<String> list = loadWeatherDict("C:\\Users\\mlyna\\Desktop\\2016Weather.csv")
//                .entrySet()
//                .stream()
//                .map(Map.Entry::getKey).sorted()
//                .collect(Collectors.toList());

//        StringBuilder str = new StringBuilder();
//        for (String s : list) {
//            str.append(s).append("\n");
//        }
//        BufferedWriter writer = new BufferedWriter(new FileWriter("filename"));
//        writer.write(str.toString());
//
//        writer.close();
//        System.out.println(loadWeatherDict("C:\\Users\\mlyna\\Desktop\\2016Weather.csv").size());
    }

    private static String getLink(String latitude, String longitude){
        String API_URL = "https://api.bigdatacloud.net/data/reverse-geocode-client?";
        return API_URL +"latitude="+latitude+"&longitude="+longitude;
    }

    private static String getDistrict(String link){
        String district = "";

        System.out.println(link);
        try {
            URL url = new URL(link);
            URLConnection request = url.openConnection();
            request.connect();

            JsonParser jp = new JsonParser();
            JsonElement root = jp.parse(new InputStreamReader((InputStream) request.getContent())); //Convert the input stream to a json element
            JsonObject rootobj = root.getAsJsonObject();
            district = rootobj.get("locality").getAsString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return district;
    }

    private static HashMap<String, Weather> loadWeatherDict(String file){
        HashMap<String, Weather> weatherDict = new HashMap<>();
        int prevHour = 0;
        int curHour;
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(file));
            String row = csvReader.readLine();
            row = csvReader.readLine();
            while (row != null) {
                String[] data = row.split(";");
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yy HH:mm");
                LocalDateTime dateAndTime = LocalDateTime.parse(data[0], formatter);
                Weather weather = new Weather(dateAndTime, data[22], Boolean.parseBoolean(data[24]), Boolean.parseBoolean(data[25]), Boolean.parseBoolean(data[26]));
                String key = String.format("%02d", dateAndTime.getMonthValue())+String.format("%02d", dateAndTime.getDayOfMonth())+String.format("%02d", dateAndTime.getHour());
                weatherDict.put(key, weather);

                curHour = dateAndTime.getHour();
                if (prevHour != curHour && (curHour != getNextHour(prevHour))) {
                    System.out.println(dateAndTime);
                    System.out.println("Previous Hour:" + prevHour + " Current Hour:" + curHour);
                    LocalDateTime newDateTime = dateAndTime;
                    for(int i = getNextHour(prevHour); i != curHour; i=getNextHour(i)) {
                        System.out.println("Handling missing hour: " + i);
                        newDateTime = newDateTime.minus(Duration.ofHours(1));
                        Weather addon = new Weather(newDateTime, data[22], Boolean.parseBoolean(data[24]), Boolean.parseBoolean(data[25]), Boolean.parseBoolean(data[26]));
                        String addonKey = String.format("%02d", newDateTime.getMonthValue())+String.format("%02d", newDateTime.getDayOfMonth())+String.format("%02d", newDateTime.getHour());
                        weatherDict.put(addonKey, addon);
                    }
                }
                prevHour = curHour;

                row = csvReader.readLine();
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return weatherDict;
    }

    private static int getNextHour(int curHour){
        if (curHour==23)
            return 0;
        else return curHour+1;
    }

}

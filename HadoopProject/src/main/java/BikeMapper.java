import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

public class BikeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0)
			return;
		else {
			String line = value.toString();
			String[] vals = line.split(",");

			StringBuilder stringBuilder = new StringBuilder();
			String duration = vals[0].replace("\"", "");
			String startTime = vals[1].replace("\"", "");
			String finishTime = vals[2].replace("\"", "");
			String stationLat = vals[5].replace("\"", "");
			String stationLong = vals[6].replace("\"", "");
			String subscription = vals[12].replace("\"", "");
			String birthYear = vals[13].replace("\"", "");
			String gender = vals[14].replace("\"", "");

			System.out.println(startTime);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS");
			LocalDateTime dateAndTime = LocalDateTime.parse(startTime, formatter);
			String dateAndTimeKey = String.format("%02d", dateAndTime.getMonthValue())+String.format("%02d", dateAndTime.getDayOfMonth())+String.format("%02d", dateAndTime.getHour());
			HashMap<String, Weather> weatherDict = loadWeatherDict("/home/cloudera/Desktop/2016Weather.csv");

			Weather weather = weatherDict.get(dateAndTimeKey);
			Boolean isSnow = weather.isSnow();
			Boolean isFog = weather.isFog();
			Boolean isRain = weather.isSnow();
			String weatherDescription = weather.getConditions();

			String districtName = getDistrict(getLink(stationLat, stationLong));

			stringBuilder.append(duration).append(',')
					.append(startTime).append(',')
					.append(finishTime).append(',')
					.append(stationLat).append(',')
					.append(stationLong).append(',')
					.append(subscription).append(',')
					.append(birthYear).append(',')
					.append(gender).append(',')
					.append(districtName).append(',')
					.append(weatherDescription).append(',').
					append(isSnow).append(',').
					append(isFog).append(',').
					append(isRain);

			context.write(key, new Text(stringBuilder.toString()));
		}
	}

//	private String getTimeOfDay(String startTime){
//
//	}
//
//	private String getAgeGroupName(String birthYear){
//
//	}
//
//	private String getSubjectiveDescription(String weatherContitions){
//
//	}
//
//	private Boolean isCloudly(String weatherContitions){
//
//	}

	private String getLink(String latitude, String longitude){
		String API_URL = "https://api.bigdatacloud.net/data/reverse-geocode-client?";
		return API_URL +"latitude="+latitude+"&longitude="+longitude;
	}

	private String getDistrict(String link){
		JsonArray array;
		String district = "NA";

//		System.out.println(link);
		try {
			URL url = new URL(link);
			URLConnection request = url.openConnection();
			request.connect();

			JsonParser jp = new JsonParser();
			JsonElement root = jp.parse(new InputStreamReader((InputStream) request.getContent())); //Convert the input stream to a json element
			JsonObject rootobj = root.getAsJsonObject();
			array = rootobj.get("localityInfo").getAsJsonObject().get("administrative").getAsJsonArray();
			for(int i = 0; i< array.size() && district.equals("NA"); i++)
				if(array.get(i).getAsJsonObject().get("description").getAsString().startsWith("borough"))
					district=array.get(i).getAsJsonObject().get("name").getAsString();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return district;
	}


	private HashMap<String, Weather> loadWeatherDict(String file){
		HashMap<String, Weather> weatherDict = new HashMap<>();
		int prevHour = 0;
		int curHour;
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader(file));
			String row = csvReader.readLine();
			row = csvReader.readLine();
			while (row != null) {
				String[] data = row.split(",");
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yy HH:mm");
				LocalDateTime dateAndTime = LocalDateTime.parse(data[0], formatter);
				Weather weather = new Weather(dateAndTime, data[22], Boolean.parseBoolean(data[24]), Boolean.parseBoolean(data[25]), Boolean.parseBoolean(data[26]));
				String key = String.format("%02d", dateAndTime.getMonthValue())+String.format("%02d", dateAndTime.getDayOfMonth())+String.format("%02d", dateAndTime.getHour());
				weatherDict.put(key, weather);

				curHour = dateAndTime.getHour();
				if (prevHour != curHour && (curHour != getNextHour(prevHour))) {
//					System.out.println(dateAndTime);
//					System.out.println("Previous Hour:" + prevHour + " Current Hour:" + curHour);
					LocalDateTime newDateTime = dateAndTime;
					for(int i = getNextHour(prevHour); i != curHour; i=getNextHour(i)) {
//						System.out.println("Handling missing hour: " + i);
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
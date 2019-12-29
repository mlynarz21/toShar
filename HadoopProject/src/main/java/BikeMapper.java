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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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

//			System.out.println(startTime);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS");
			LocalDateTime dateAndTime = LocalDateTime.parse(startTime, formatter);
			String dateAndTimeKey = String.format("%02d", dateAndTime.getMonthValue()) + String.format("%02d", dateAndTime.getDayOfMonth()) + String.format("%02d", dateAndTime.getHour());
//			HashMap<String, Weather> weatherDict = loadWeatherDict("/home/cloudera/Desktop/2016Weather.csv");

//			Weather weather = weatherDict.get(dateAndTimeKey);
//			Boolean isSnow = weather.isSnow();
//			Boolean isFog = weather.isFog();
//			Boolean isRain = weather.isRain();
//			String weatherDescription = weather.getConditions();

			String districtName = getDistrict(getLink(stationLat, stationLong));

//			TODO to mogl byc etap1, ale jak zlaczymy to to co ponizej
//			stringBuilder
//					.append(duration).append(',')
//					.append(startTime).append(',')
//					.append(finishTime).append(',')
//					.append(stationLat).append(',')
//					.append(stationLong).append(',')
//					.append(subscription).append(',')
//					.append(birthYear).append(',')
//					.append(gender).append(',')
//					.append(districtName).append(',')
//					.append(weatherDescription).append(',').
//					append(isSnow).append(',').
//					append(isFog).append(',').
//					append(isRain);

			stringBuilder
					.append(duration).append(',')
//					.append(isFog).append(',')
//					.append(isRain).append(',')
//					.append(isSnow).append(',')
//					.append(isCloudly(weatherDescription)).append(',')
					.append(stationLat).append(',')
					.append(stationLong).append(',')
					.append(subscription).append(',')
					.append(getTimeOfDay(startTime)).append(',')
					.append(getAgeGroupName(birthYear)).append(',')
					.append(getGender(gender)).append(',');
//					.append(districtName).append(',')
//					.append(weatherDescription).append(',')
//					.append(getSubjectiveDescription(weatherDescription));

			context.write(key, new Text(stringBuilder.toString()));
		}
	}

	private String getTimeOfDay(String startTime) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS");
		LocalDateTime dateAndTime = LocalDateTime.parse(startTime, formatter);
		if ((dateAndTime.getHour() > 6 || dateAndTime.getHour() == 6 && dateAndTime.getMinute() > 0) && dateAndTime.getHour() < 22 || dateAndTime.getHour() == 22 && dateAndTime.getMinute() == 0)
			return "Day";
		else return "Night";
	}

	private String getAgeGroupName(String birthYear) {

		int birthYr = Integer.valueOf(birthYear);

		if (birthYr >= 2004)
			return "Child";
		else if (birthYr >= 1998)
			return "Youth";
		else if (birthYr >= 1986)
			return "Young";
		else if (birthYr >= 1961)
			return "Mature";
		else return "Elderly";
	}

	private String getSubjectiveDescription(String weatherConditions) {
		List<String> goodConditions = new ArrayList<>(Arrays.asList("Clear", "Haze", "Overcast", "Scattered Clouds", "Partly Cloudly", "Mostly Cloudly"));
		List<String> badConditions = new ArrayList<>(Arrays.asList("Fog", "Light Freezing Fog", "Light Snow", "Snow", "Heavy Snow", "Light Rain", "Rain", "Light Freezing Rain", "Heavy Rain"));

		return goodConditions.contains(weatherConditions) ? "Good" : badConditions.contains(weatherConditions) ? "Bad" : "Unknown";
	}

	private Boolean isCloudly(String weatherConditions) {
		List<String> cloudyConditions = new ArrayList<>(Arrays.asList("Overcast", "Scattered Clouds", "Partly Cloudly", "Mostly Cloudly"));
		return cloudyConditions.contains(weatherConditions);
	}

	private String getGender(String genderID) {
		int genderId = Integer.valueOf(genderID);
		switch (genderId) {
			case 1:
				return "Man";
			case 2:
				return "Woman";
			default:
				return "Unknown";
		}
	}

	private String getLink(String latitude, String longitude) {
		String API_URL = "https://api.bigdatacloud.net/data/reverse-geocode-client?";
		return API_URL + "latitude=" + latitude + "&longitude=" + longitude;
	}

	private String getDistrict(String link) {
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
			for (int i = 0; i < array.size() && district.equals("NA"); i++)
				if (array.get(i).getAsJsonObject().get("description").getAsString().startsWith("borough"))
					district = array.get(i).getAsJsonObject().get("name").getAsString();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return district;
	}


	private static HashMap<String, Weather> loadWeatherDict(String file) {
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
				Weather weather = new Weather(dateAndTime, data[22], data[24].equals("1"), data[25].equals("1"), data[26].equals("1"));
				String key = String.format("%02d", dateAndTime.getMonthValue()) + String.format("%02d", dateAndTime.getDayOfMonth()) + String.format("%02d", dateAndTime.getHour());
				weatherDict.put(key, weather);

				curHour = dateAndTime.getHour();
				if (prevHour != curHour && (curHour != getNextHour(prevHour))) {
//					System.out.println(dateAndTime);
//					System.out.println("Previous Hour:" + prevHour + " Current Hour:" + curHour);
					LocalDateTime newDateTime = dateAndTime;
					for (int i = getNextHour(prevHour); i != curHour; i = getNextHour(i)) {
//						System.out.println("Handling missing hour: " + i);
						newDateTime = newDateTime.minus(Duration.ofHours(1));
						Weather addon = new Weather(newDateTime, data[22], data[24].equals("1"), data[25].equals("1"), data[26].equals("1"));
						String addonKey = String.format("%02d", newDateTime.getMonthValue()) + String.format("%02d", newDateTime.getDayOfMonth()) + String.format("%02d", newDateTime.getHour());
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

	private static int getNextHour(int curHour) {
		if (curHour == 23)
			return 0;
		else return curHour + 1;
	}
}
package com.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class RestClientPostMessages {

	public static void main(String[] args) {

	  try {

			Properties properties = loadProperties("/config.properties");
			URL url = new URL(properties.getProperty("base.url")+"/topics/"+properties.getProperty("topic.name"));
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json");
			  conn.setRequestProperty("Content-Type", "application/vnd.kafka.avro.v2+json");

			String messageTemplate = readFile(properties.getProperty("template.path"));
		  	String keySchema = readFile(properties.getProperty("resource.base.path") + properties.getProperty("key.schema.path"));
		  	String valueSchema = readFile(properties.getProperty("resource.base.path") + properties.getProperty("value.schema.path"));
		  	String key = readFile(properties.getProperty("resource.base.path") + properties.getProperty("key.path"));
		  	String value = readFile(properties.getProperty("resource.base.path") + properties.getProperty("value.path"));

		  	String input = String.format(messageTemplate, keySchema, valueSchema, key, value);

			OutputStream os = conn.getOutputStream();
			os.write(input.getBytes());
			os.flush();

			if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
				throw new RuntimeException("Failed : HTTP error code : "
					+ conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));

			String output;
			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			conn.disconnect();

	  } catch (MalformedURLException e) {
		e.printStackTrace();
	  } catch (IOException e) {
		e.printStackTrace();
	 }

	}

	private static Properties loadProperties(String filePath) {
		try (InputStream input = RestClientPostMessages.class.getResourceAsStream(filePath)) {
			Properties prop = new Properties();
			// load a properties file
			prop.load(input);
			return prop;
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return null;
	}

	static String readFile(String filePath)
			throws IOException
	{
		File file = new File(RestClientPostMessages.class.getResource(filePath).getPath());
		//init array with file length
		byte[] bytesArray = new byte[(int) file.length()];

		InputStream fis = new FileInputStream(file);
		fis.read(bytesArray); //read file into bytes[]
		fis.close();

		return new String(bytesArray);
	}

}

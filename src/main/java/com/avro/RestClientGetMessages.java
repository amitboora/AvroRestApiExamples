package com.avro;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class RestClientGetMessages {

	public static final String CONSGRP = "consgrp";
	public static final String CONSUMER_INSTANCE = "amit_consumer";
	private static String hostUrl = "https://kafka-1-rest.nwsl.site";
	//private static String hostUrl = "http://localhost:8082";
	private static String topicName = "ci-1-NBS-Speedlayer-CIS-Statement";

	// http://localhost:8080/RESTfulExample/json/product/get
	public static void main(String[] args) {

	  //createInstance();
	  //subscribeTopic();
	  getMessages();
	  //commitOffset();
	}

	private static void getMessages(){
		try {

			URL url = new URL(hostUrl+ "/consumers/" + CONSGRP + "/instances/" + CONSUMER_INSTANCE + "/records");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/vnd.kafka.avro.v2+json");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode() + conn.getResponseMessage());
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

	private static void createInstance(){
		try {

			String consumerCreateRequest = "{\n" +
					"  \"name\": \"" + CONSUMER_INSTANCE + "\",\n" +
					"  \"format\": \"avro\",\n" +
					//"  \"auto.offset.reset\": \"earliest\",\n" +
					"  \"auto.offset.reset\": \"latest\",\n" +
					"  \"auto.commit.enable\": \"false\"\n" +
					"}";

			//System.out.println(consumerCreateRequest);

			Properties properties = loadProperties("/config.properties");
			URL url = new URL(hostUrl+ "/consumers/" + CONSGRP);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json");
			conn.setRequestProperty("Content-Type", "application/vnd.kafka.avro.v2+json");


			String input = consumerCreateRequest;

			OutputStream os = conn.getOutputStream();
			os.write(input.getBytes());
			os.flush();

			if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode() + conn.getResponseMessage());
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

	private static void commitOffset(){
		try {

			String commitOffsetRequest = "{\n" +
					"  \"offsets\": [\n" +
					"    {\n" +
					"      \"topic\": \""+topicName+"\",\n" +
					"      \"partition\": 0,\n" +
					"      \"offset\": 37\n" +
					"    }\n" +
					"  ]\n" +
					"}";

			URL url = new URL(hostUrl+ "/consumers/" + CONSGRP + "/instances/" + CONSUMER_INSTANCE + "/offsets");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json");
			conn.setRequestProperty("Content-Type", "application/vnd.kafka.v2+json");


			String input = commitOffsetRequest;

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

	private static void subscribeTopic(){
		try {

			String messageRequest = "{\n" +
					"  \"topics\": [\n" +
					"    \""+topicName+"\"\n" +
					"  ]\n" +
					"}";

			URL url = new URL(hostUrl+ "/consumers/" + CONSGRP + "/instances/" + CONSUMER_INSTANCE + "/subscription");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json");
			conn.setRequestProperty("Content-Type", "application/vnd.kafka.v2+json");


			String input = messageRequest;

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

}

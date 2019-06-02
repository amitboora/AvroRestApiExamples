package com.avro;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class RestClientGetTopics {

	private static String msg = "{\"type\":\"record\",\"namespace\":\"com.nbs.speedlayer.sap.avro\",\"name\":\"Account\",\"fields\":[{\"name\":\"CLIENT\",\"type\":[\"null\",\"string\"],\"doc\":\"Client\"},{\"name\":\"CONTRACT_INT\",\"type\":[\"null\",\"string\"],\"doc\":\"Internal Contract ID\"},{\"name\":\"AC_CHANGE_TSTAMP\",\"type\":[\"null\",\"double\"],\"doc\":\"Entry/Change Time Stamp - Long\"},{\"name\":\"VALID_FROM\",\"type\":[\"null\",\"double\"],\"doc\":\"Validity Start (Time Stamp)\"},{\"name\":\"VALID_TO\",\"type\":[\"null\",\"double\"],\"doc\":\"Validity End (Time Stamp)\"},{\"name\":\"VALID_TO_REAL\",\"type\":[\"null\",\"double\"],\"doc\":\"Actual Validity End (Time Stamp)\"},{\"name\":\"ACNUM_EXT\",\"type\":[\"null\",\"string\"],\"doc\":\"Account Number\"},{\"name\":\"BANKLAND\",\"type\":[\"null\",\"string\"],\"doc\":\"Bank country key\"},{\"name\":\"BANKKEY\",\"type\":[\"null\",\"string\"],\"doc\":\"Bank Key\"},{\"name\":\"CN_CUR\",\"type\":[\"null\",\"string\"],\"doc\":\"Contract Currency\"},{\"name\":\"CN_USAGE\",\"type\":[\"null\",\"string\"],\"doc\":\"Account Differentiation Characteristic\"},{\"name\":\"XCHKDIG\",\"type\":[\"null\",\"string\"],\"doc\":\"Deactivate Check Digit Calculation\"},{\"name\":\"CHANGE_USER\",\"type\":[\"null\",\"string\"],\"doc\":\"Last Changed by\"},{\"name\":\"CHANGE_BTCATG\",\"type\":[\"null\",\"string\"],\"doc\":\"Business Transaction Category\"},{\"name\":\"FLG_TOBE_RELD\",\"type\":[\"null\",\"string\"],\"doc\":\"Contract Release Status\"},{\"name\":\"IBAN\",\"type\":[\"null\",\"string\"],\"doc\":\"International Bank Account Number\"},{\"name\":\"KEYCODE\",\"type\":[\"null\",\"string\"],\"doc\":\"Identification Key\"},{\"name\":\"BAC_ID\",\"type\":[\"null\",\"string\"],\"doc\":\"Identification\"}]}";

	// http://localhost:8080/RESTfulExample/json/product/get
	public static void main(String[] args) {

	  try {

		URL url = new URL("https://kafka-1-rest.nwsl.site/topics");
		  //URL url = new URL("http://localhost:8082/topics");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");

		if (conn.getResponseCode() != 200) {
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

}

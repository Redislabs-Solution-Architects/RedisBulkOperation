package com.redis;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;

public class RBOConfig {
	
	private static final String operationPropertyName = "operation";
	private static final String hostPropertyName = "host";
	private static final String portPropertyName = "port";
	private static final String userPropertyName = "user";
	private static final String passwordPropertyName = "password";
	private static final String verbosePropertyName = "verbose";
	private static final String sslPropertyName = "ssl";
	private static final String sniPropertyName = "sni";
	private static final String patternPropertyName = "pattern";
	private static final String bulkPropertyName = "bulk";

	
	private static final String [] propertyNames = {
			"operation",
			"host",
			"port",
			"user",
			"password",
			"verbose",
			"ssl",
			"sni",
			"pattern",
			"bulk"
	};
	
	private String operation = "Count";
	private String host = "127.0.0.1";
	private int port = 6379;
	private String user = "";
	private String password = "";
	private boolean verbose = false;
	private boolean ssl = false;
	private String sni = "";
	private String pattern = "*";
	private int bulk = 1000;

	private Properties properties = new Properties();
	
	static RBOConfig instance = new RBOConfig();
	
	public static RBOConfig getConfig()
	{
		return instance;
	}
	
	public void load(String fileName) {
		File file = new File(fileName);
		FileInputStream fileInput;
		try {
			fileInput = new FileInputStream(file);
			properties.load(fileInput);
			fileInput.close();

		} catch (FileNotFoundException e) {
			System.out.println("Warning: Configuration file " + fileName + " Not Found. " + e.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (properties.containsKey(operationPropertyName))
			operation = properties.getProperty(operationPropertyName);

		if (properties.containsKey(hostPropertyName))
			host = properties.getProperty(hostPropertyName);

		if (properties.containsKey(portPropertyName))
			port = Integer.parseInt(properties.getProperty(portPropertyName));

		if (properties.containsKey(userPropertyName))
			user = properties.getProperty(userPropertyName);

		if (properties.containsKey(passwordPropertyName))
			password = properties.getProperty(passwordPropertyName);

		verbose = getBooleanProperty(verbosePropertyName);

		ssl = getBooleanProperty(sslPropertyName);

		if (properties.containsKey(sniPropertyName))
			sni = properties.getProperty(sniPropertyName);

		if (properties.containsKey(patternPropertyName))
			pattern = properties.getProperty(patternPropertyName);

		if (properties.containsKey(bulkPropertyName))
			bulk = Integer.parseInt(properties.getProperty(bulkPropertyName));
	}

	private boolean getBooleanProperty(String property) {
		if (properties.containsKey(property)) {
			String str = properties.getProperty(property);
			if (str != null && !str.isEmpty() && (str.equalsIgnoreCase("TRUE") || str.equalsIgnoreCase("YES") || str.equalsIgnoreCase("1")))
				return true;
		}
		return false;
	}
	public void printConfig()
	{
		System.out.println(properties);
	}
	
	public String getOperation() {
		return operation;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public boolean isVerbose() {
		return verbose;
	}
	public boolean isSSL() {
		return ssl;
	}

	public String getSNI() {
		return sni;
	}

	public String getPattern() {
		return pattern;
	}

	public int getBulk() {
		return bulk;
	}
	public String getProperty(String name) {
		return properties.getProperty(name);
	}

}

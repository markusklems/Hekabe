package com.hekabe.dashboard.shared;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;

public class User {

	private String username;
	
	private String password;
	
	private String EC2AccessKey;
	
	private String EC2AccessKeySecret;

	
	
	/**
	 * @param username
	 * @param password
	 * @param eC2AccessKey
	 * @param eC2AccessKeySecret
	 */
	public User(String username, String password, String eC2AccessKey,
			String eC2AccessKeySecret) {
		super();
		this.username = username;
		this.password = password;
		EC2AccessKey = eC2AccessKey;
		EC2AccessKeySecret = eC2AccessKeySecret;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @return the eC2AccessKey
	 */
	public String getEC2AccessKey() {
		return EC2AccessKey;
	}

	/**
	 * @return the eC2AccessKeySecret
	 */
	public String getEC2AccessKeySecret() {
		return EC2AccessKeySecret;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @param eC2AccessKey the eC2AccessKey to set
	 */
	public void setEC2AccessKey(String eC2AccessKey) {
		EC2AccessKey = eC2AccessKey;
	}

	/**
	 * @param eC2AccessKeySecret the eC2AccessKeySecret to set
	 */
	public void setEC2AccessKeySecret(String eC2AccessKeySecret) {
		EC2AccessKeySecret = eC2AccessKeySecret;
	}
	
	
	public BasicAWSCredentials getCredentials() {
		return new BasicAWSCredentials(EC2AccessKey, EC2AccessKeySecret);
	}
	
	
}

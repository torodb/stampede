package com.torodb.config.backend.postgres;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.Config;
import com.torodb.config.backend.BackendImplementation;

/**
 * PostgreSQL configuration (default)
 */
@XmlType(namespace=Config.TOROCONFIG_NAMESPACE,
propOrder={})
@JsonPropertyOrder({
	"host",
	"port",
	"database",
	"user",
	"password",
	"toropassFile",
	"applicationName"
})
public class Postgres extends BackendImplementation {
	/**
	 * The host or ip associated to the network interface where clients will connect
	 */
	private String host = "localhost";
	/**
	 * The port where the clients will connect
	 */
	private int port = 5432;
	/**
	 * The database that will be used
	 */
	@Deprecated
	private String database = "torod";
	/**
	 * The user that will connect to the database
	 */
	private String user = "torodb";
	/**
	 * Specify password that will be used to connect to the database
	 */
	private String password;
	/**
	 * You can specify a file that use .pgpass syntax: <host>:<port>:<database>:<user>:<password> (can have multiple lines)
	 */
	private String toropassFile = System.getProperty("user.home") + "/.toropass";
	/**
	 * The application name used by driver that will connect to the database
	 */
	private String applicationName = "toro";
	
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getToropassFile() {
		return toropassFile;
	}
	public void setToropassFile(String toropassFile) {
		this.toropassFile = toropassFile;
	}
	/*
	 * TODO: This should disappear
	 */
	@Deprecated
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getDatabase() {
		return database;
	}
	/*
	 * TODO: This should disappear
	 */
	@Deprecated
	public void setDatabase(String database) {
		this.database = database;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getApplicationName() {
		return applicationName;
	}
	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}
}

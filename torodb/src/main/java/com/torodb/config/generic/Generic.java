package com.torodb.config.generic;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.Config;

/**
 * Generic ToroDB configuration
 */
@XmlType(namespace=Config.TOROCONFIG_NAMESPACE,
propOrder={})
@JsonPropertyOrder({
	"logLevel",
	"logFile",
	"connectionPoolSize",
	"reservedReadPoolSize"
})
public class Generic {
	
	@XmlType(namespace=Config.TOROCONFIG_NAMESPACE)
	@XmlEnum
	public enum LogLevel {
		NONE,
		INFO,
		ERROR,
		WARNING,
		DEBUG,
		TRACE;
	}
	
	/**
	 * Level of log emitted
	 */
	private LogLevel logLevel = LogLevel.WARNING;
	/**
	 * File where log will be written
	 */
	private String logFile;
	/**
	 * Maximum number of connections to establish to the database. It must be higher or equal than 3
	 */
	private int connectionPoolSize = 30;
	/**
	 * Reserved connections that will be reserved to store global cursors. It must be lower than total connections minus 2
	 */
	private int reserverdReadPoolSize = 10;
	
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public LogLevel getLogLevel() {
		return logLevel;
	}
	public void setLogLevel(LogLevel logLevel) {
		this.logLevel = logLevel;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getLogFile() {
		return logFile;
	}
	public void setLogFile(String logFile) {
		this.logFile = logFile;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public int getConnectionPoolSize() {
		return connectionPoolSize;
	}
	public void setConnectionPoolSize(int connectionPoolSize) {
		this.connectionPoolSize = connectionPoolSize;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public int getReserverdReadPoolSize() {
		return reserverdReadPoolSize;
	}
	public void setReserverdReadPoolSize(int reserverdReadPoolSize) {
		this.reserverdReadPoolSize = reserverdReadPoolSize;
	}
}

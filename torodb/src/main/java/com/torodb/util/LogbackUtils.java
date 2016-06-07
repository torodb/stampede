/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.LoggerFactory;

import com.torodb.config.model.generic.LogLevel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.joran.spi.JoranException;

public class LogbackUtils {

	public static void appendToLogFile(String logFile) {
		LoggerContext loggerContext = getLoggerContext();
		Logger root = getRootLogger();
		String pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{35} - %msg %n";
		Iterator<Appender<ILoggingEvent>> iteratorForAppenders = root.iteratorForAppenders();
		while(iteratorForAppenders.hasNext()) {
			Appender<ILoggingEvent> appender = iteratorForAppenders.next();
			if (appender instanceof OutputStreamAppender) {
				Encoder<ILoggingEvent> encoder = ((OutputStreamAppender<ILoggingEvent>) appender).getEncoder();
				if (encoder instanceof PatternLayoutEncoder) {
					pattern = ((PatternLayoutEncoder) encoder).getPattern();
					break;
				}
			}
		}
		PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();
		patternLayoutEncoder.setContext(loggerContext);
		patternLayoutEncoder.setPattern(pattern);
		patternLayoutEncoder.start();
		
		FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
		fileAppender.setFile(logFile);
		fileAppender.setContext(loggerContext);
		fileAppender.setEncoder(patternLayoutEncoder);
		fileAppender.start();
		
		root.addAppender(fileAppender);
	}

	public static void reconfigure(String logbackFile) throws JoranException {
		LoggerContext loggerContext = getLoggerContext();
		
		loggerContext.reset();
		
		JoranConfigurator joranConfigurator = new JoranConfigurator();
		joranConfigurator.setContext(loggerContext);
		joranConfigurator.doConfigure(logbackFile);
	}

	public static void setLogPackages(Map<String, LogLevel> logPackages) {
		for (Entry<String, LogLevel> logPackage : logPackages.entrySet()) {
			setLoggerLevel((Logger) LoggerFactory.getLogger(logPackage.getKey()), logPackage.getValue());
		}
	}

	public static LoggerContext getLoggerContext() {
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		return loggerContext;
	}

	public static Logger getRootLogger() {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		return root;
	}

	public static void setLoggerLevel(Logger logger, LogLevel logLevel) {
		switch(logLevel) {
		case NONE:
			logger.setLevel(Level.OFF);
			break;
		case INFO:
			logger.setLevel(Level.INFO);
			break;
		case ERROR:
			logger.setLevel(Level.ERROR);
			break;
		case WARNING:
			logger.setLevel(Level.WARN);
			break;
		case DEBUG:
			logger.setLevel(Level.DEBUG);
			break;
		case TRACE:
			logger.setLevel(Level.ALL);
			break;
		}
	}

}

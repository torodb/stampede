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

package com.torodb.packaging.util;

import java.io.File;
import java.io.Serializable;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

import com.torodb.core.exceptions.SystemException;
import com.torodb.packaging.config.model.generic.LogLevel;

public class Log4jUtils {

    public static void appendToLogFile(String logFile) {
        LoggerContext coreContext = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = coreContext.getConfiguration();
        final Layout<? extends Serializable> layout = PatternLayout.newBuilder()
                .withConfiguration(configuration)
                .withPattern("%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n")
                .build();
        final Appender appender = FileAppender.createAppender(logFile, "false", "false", "File", "true",
                "false", null, null, layout, null, "false", null, configuration);
        appender.start();
        for (LoggerConfig loggerConfig : configuration.getLoggers().values()) {
            configuration.addLoggerAppender(coreContext.getLogger(loggerConfig.getName()), appender);
        }
    }

    public static void reconfigure(String configurationFile) {
        try {
            LoggerContext coreContext = (LoggerContext) LogManager.getContext(false);
            coreContext.setConfigLocation(new File(configurationFile).toURI());
        } catch (Exception ex) {
            throw new SystemException(ex);
        }
    }

    public static void setLogPackages(Map<String, LogLevel> logPackages) {
        for (Map.Entry<String, LogLevel> logPackage : logPackages.entrySet()) {
            LoggerContext coreContext = (LoggerContext) LogManager.getContext(false);
            Configuration configuration = coreContext.getConfiguration();
            org.apache.logging.log4j.LogManager.getLogger(logPackage.getKey());
            if (configuration.getLoggers().containsKey(logPackage.getKey())) {
                setLevel(configuration.getLoggerConfig(logPackage.getKey()), logPackage.getValue());
            }
        }
    }

    public static void setRootLevel(LogLevel logLevel) {
        LoggerContext coreContext = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = coreContext.getConfiguration();
        for (LoggerConfig loggerConfig : configuration.getLoggers().values()) {
            setLevel(loggerConfig, logLevel);
        }
    }
    
    private static void setLevel(LoggerConfig loggerConfig, LogLevel logLevel) {
        switch (logLevel) {
        case NONE:
            loggerConfig.setLevel(Level.OFF);
            break;
        case INFO:
            loggerConfig.setLevel(Level.INFO);
            break;
        case ERROR:
            loggerConfig.setLevel(Level.ERROR);
            break;
        case WARNING:
            loggerConfig.setLevel(Level.WARN);
            break;
        case DEBUG:
            loggerConfig.setLevel(Level.DEBUG);
            break;
        case TRACE:
            loggerConfig.setLevel(Level.TRACE);
            break;
        }
    }

    public static void addRootAppenderListener(AppenderListener appenderListener) {
        org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager
                .getRootLogger();
        org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;

        InternalAppenderListener internalAppenderListener = new InternalAppenderListener(appenderListener);
        internalAppenderListener.start();
        coreLogger.addAppender(internalAppenderListener);
    }

    public interface AppenderListener {
        void listen(String text, Throwable throwable);
    }
    
    private static class InternalAppenderListener extends AbstractAppender {

        private static final long serialVersionUID = 1L;
        
        private final AppenderListener appenderListener;
        
        protected InternalAppenderListener(AppenderListener appenderListener) {
            super("APPNEDER_LISTENER", null, null);
            this.appenderListener = appenderListener;
        }

        @Override
        public void append(LogEvent event) {
            appenderListener.listen(event.getMessage().getFormattedMessage(), event.getThrown());
        }
    }
}

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

import java.io.FileInputStream;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.layout.PatternLayout;

import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.core.exceptions.SystemException;

public class Log4jUtils {

    public static void appendToLogFile(String logFile) {
        org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager
                .getRootLogger();
        org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;

        org.apache.logging.log4j.core.appender.FileAppender fileAppender = 
                org.apache.logging.log4j.core.appender.FileAppender.createAppender(
                    logFile, null, null, "FILE", null, null, null, null,
                    PatternLayout.newBuilder().withPattern("").build(), null, null, null, null);
        fileAppender.start();
        coreLogger.addAppender(fileAppender);
    }

    public static void reconfigure(String configurationFile) {
        try {
            org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager
                    .getRootLogger();
            org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;
            
            XmlConfiguration xmlConfiguration = new XmlConfiguration(
                    new ConfigurationSource(new FileInputStream(configurationFile)));
            
            LoggerContext coreContext = coreLogger.getContext();
            coreContext.stop();
            coreContext.start(xmlConfiguration);
        } catch (Exception ex) {
            throw new SystemException(ex);
        }
    }

    public static void setLogPackages(Map<String, LogLevel> logPackages) {
        for (Map.Entry<String, LogLevel> logPackage : logPackages.entrySet()) {
            org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager
                    .getLogger(logPackage.getKey());
            org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;
            setLevel(coreLogger, logPackage.getValue());
        }
    }

    public static void setRootLevel(LogLevel logLevel) {
        org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager
                .getRootLogger();
        org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;
        setLevel(coreLogger, logLevel);
    }
    
    private static void setLevel(org.apache.logging.log4j.core.Logger coreLogger, LogLevel logLevel) {
        switch (logLevel) {
        case NONE:
            coreLogger.setLevel(Level.OFF);
            break;
        case INFO:
            coreLogger.setLevel(Level.INFO);
            break;
        case ERROR:
            coreLogger.setLevel(Level.ERROR);
            break;
        case WARNING:
            coreLogger.setLevel(Level.WARN);
            break;
        case DEBUG:
            coreLogger.setLevel(Level.DEBUG);
            break;
        case TRACE:
            coreLogger.setLevel(Level.TRACE);
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

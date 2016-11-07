package com.torodb.packaging.config.model.generic.log4j;

import com.torodb.packaging.config.model.generic.LogLevel;
import org.apache.logging.log4j.Level;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Log4jLevelToLogLevelMapper {

    private static final Map<Level, LogLevel> DICTIONARY = Collections.unmodifiableMap(Stream.of(
            new AbstractMap.SimpleEntry<Level, LogLevel>(Level.OFF, LogLevel.NONE),
            new AbstractMap.SimpleEntry<Level, LogLevel>(Level.ERROR, LogLevel.ERROR),
            new AbstractMap.SimpleEntry<Level, LogLevel>(Level.WARN, LogLevel.WARNING),
            new AbstractMap.SimpleEntry<Level, LogLevel>(Level.INFO, LogLevel.INFO),
            new AbstractMap.SimpleEntry<Level, LogLevel>(Level.DEBUG, LogLevel.DEBUG),
            new AbstractMap.SimpleEntry<Level, LogLevel>(Level.TRACE, LogLevel.TRACE))
            .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue())
            ));
    private static final Map<LogLevel, Level> INVERSE_DICTIONARY = DICTIONARY.entrySet().stream()
            .collect(Collectors.toMap((e) -> e.getValue(), (e) -> e.getKey()));

    public static LogLevel map(Level log4jLevel) {
        return DICTIONARY.get(log4jLevel);
    }

    public static Level map(LogLevel logLevel) {
        return INVERSE_DICTIONARY.get(logLevel);
    }

}


/*
 * ToroDB - ToroDB-poc: Integration Tests
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.integration;

import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Objects;

import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.core.exceptions.SystemException;

/**
 * Represents the state where the integration test want to be executed.
 */
public class IntegrationTestEnvironment {

    private final Protocol protocol;
    private final Backend backend;
    private final LogLevel logLevel;

    public static final IntegrationTestEnvironment CURRENT_INTEGRATION_TEST_ENVIRONMENT =
            new IntegrationTestEnvironment(currentProtocol(), currentBackend(), currentLogLevel());

    public IntegrationTestEnvironment(Protocol protocol, Backend backend, LogLevel logLevel) {
        this.protocol = protocol;
        this.backend = backend;
        this.logLevel = logLevel;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public Backend getBackend() {
        return backend;
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

	private static Protocol currentProtocol() {
		Protocol currentProtocol = Protocol.MONGO;

		String currentProtocolValue = System.getenv(Protocol.class.getSimpleName());
		if (currentProtocolValue != null) {
			currentProtocol = valueOf(Protocol.class, currentProtocolValue);
		}

		return currentProtocol;
	}

    private static Backend currentBackend() {
        Backend currentBackend = Backend.DERBY;

        String currentBackendValue = System.getenv(Backend.class.getSimpleName());
        if (currentBackendValue != null) {
            currentBackend = valueOf(Backend.class, currentBackendValue);
        }

        return currentBackend;
    }

    private static LogLevel currentLogLevel() {
        LogLevel currentLogLevel = LogLevel.INFO;

        String currentLogLevelValue = System.getenv(LogLevel.class.getSimpleName());
        if (currentLogLevelValue != null) {
            currentLogLevel = valueOf(LogLevel.class, currentLogLevelValue);
        }

        return currentLogLevel;
    }

    private static <E extends Enum<?>> E valueOf(Class<E> enumClass, String value) {
        value = value.toUpperCase(Locale.ENGLISH);
        
        try {
            Method values = enumClass.getMethod("values");
            
            Object[] enumValues = Object[].class.cast(values.invoke(null));
            
            for (Object enumObject : enumValues) {
                E enumValue = enumClass.cast(enumObject);
                if (enumValue.name().toUpperCase(Locale.ENGLISH).equals(value)) {
                    return enumValue;
                }
            }
        } catch(Exception exception) {
            throw new SystemException(exception);
        }
        
        throw new IllegalArgumentException("Value " + value + " is not a valid " + enumClass.getSimpleName());
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + Objects.hashCode(this.protocol);
        hash = 97 * hash + Objects.hashCode(this.backend);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final IntegrationTestEnvironment other = (IntegrationTestEnvironment) obj;
        if (this.protocol != other.protocol) {
            return false;
        }
        if (this.backend != other.backend) {
            return false;
        }
        return true;
    }
}

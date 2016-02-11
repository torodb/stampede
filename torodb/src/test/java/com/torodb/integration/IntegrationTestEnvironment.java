/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with torodb. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.integration;

import java.util.Locale;
import java.util.Objects;

/**
 * Represents the state where the integration test want to be executed.
 */
public class IntegrationTestEnvironment {

    private final Protocol protocol;
    private final Backend backend;

    public static final IntegrationTestEnvironment CURRENT_INTEGRATION_TEST_ENVIRONMENT =
            new IntegrationTestEnvironment(currentProtocol(), currentBackend());

    public IntegrationTestEnvironment(Protocol protocol, Backend backend) {
        this.protocol = protocol;
        this.backend = backend;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public Backend getBackend() {
        return backend;
    }

	private static Protocol currentProtocol() {
		Protocol currentProtocol = Protocol.MONGO;

		String currentProtocolValue = System.getenv(Protocol.class.getSimpleName());
		if (currentProtocolValue != null) {
			currentProtocol = Protocol.valueOf(currentProtocolValue.toUpperCase(Locale.ENGLISH));
		}

		return currentProtocol;
	}

	private static Backend currentBackend() {
		Backend currentBackend = Backend.POSTGRES;

		String currentBackendValue = System.getenv(Backend.class.getSimpleName());
		if (currentBackendValue != null) {
			currentBackend = Backend.valueOf(currentBackendValue.toUpperCase(Locale.ENGLISH));
		}

		return currentBackend;
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

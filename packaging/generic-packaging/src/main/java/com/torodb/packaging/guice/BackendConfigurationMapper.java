package com.torodb.packaging.guice;

import javax.annotation.concurrent.ThreadSafe;

import com.google.inject.Inject;
import com.torodb.backend.BackendConfiguration;

@ThreadSafe
public abstract class BackendConfigurationMapper implements BackendConfiguration {

    private final long cursorTimeout;
    private final long connectionPoolTimeout;
    private final int connectionPoolSize;
    private final int reservedReadPoolSize;
	private final String dbHost;
	private final int dbPort;
	private final String dbName;
	private final String username;
	private final String password;
	private boolean includeForeignKeys;
	
	@Inject
    public BackendConfigurationMapper(long cursorTimeout, long connectionPoolTimeout, int connectionPoolSize,
            int reservedReadPoolSize, String dbHost, int dbPort, String dbName, String username, String password,
            boolean includeForeignKeys) {
        super();
        this.cursorTimeout = cursorTimeout;
        this.connectionPoolTimeout = connectionPoolTimeout;
        this.connectionPoolSize = connectionPoolSize;
        this.reservedReadPoolSize = reservedReadPoolSize;
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;
        this.username = username;
        this.password = password;
        this.includeForeignKeys = includeForeignKeys;
    }

	@Override
    public long getCursorTimeout() {
        return cursorTimeout;
    }

    @Override
    public long getConnectionPoolTimeout() {
        return connectionPoolTimeout;
    }

    @Override
	public int getConnectionPoolSize() {
		return connectionPoolSize;
	}

	@Override
	public int getReservedReadPoolSize() {
		return reservedReadPoolSize;
	}

	@Override
	public String getUsername() {
		return username;
	}

	@Override
	public String getPassword() {
		return password;
	}

	@Override
	public String getDbHost() {
		return dbHost;
	}

	@Override
	public String getDbName() {
		return dbName;
	}

	@Override
	public int getDbPort() {
		return dbPort;
	}

    @Override
    public boolean includeForeignKeys() {
        return includeForeignKeys;
    }
}

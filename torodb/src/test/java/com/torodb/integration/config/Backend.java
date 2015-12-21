package com.torodb.integration.config;

public enum Backend {
	Postgres,
	Greenplum;
	
	public static final Backend CURRENT = currentBackend();
	
	private static final Backend currentBackend() {
		Backend currentBackend = Backend.Postgres;
		
		String currentBackendValue = System.getenv(Backend.class.getName());
		if (currentBackendValue != null) {
			currentBackend = Backend.valueOf(currentBackendValue);
		}
		
		return currentBackend;
	}
}

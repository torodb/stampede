package com.torodb.integration.config;

public enum Backend {
	Postgres,
	Greenplum;
	
	public static final Backend CURRENT = currentBackend();
	
	private static final Backend currentBackend() {
		Backend currentBackend = Backend.Postgres;
		
		String currentBackendValue = System.getenv(Backend.class.getSimpleName());
		if (currentBackendValue != null) {
			currentBackend = Backend.valueOf(currentBackendValue);
		}
		
		return currentBackend;
	}
	
	private final Backend baseBackend;
	
	private Backend() {
		this.baseBackend = this;
	}
	
	private Backend(Backend baseBackend) {
		this.baseBackend = baseBackend;
	}
	
	public Backend baseBackend() {
		return baseBackend;
	}
}

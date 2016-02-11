package com.torodb.integration;

public enum Backend {
	POSTGRES,
	GREENPLUM;
	
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

package com.torodb.config.backend;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.torodb.config.Config;
import com.torodb.config.backend.greenplum.Greenplum;
import com.torodb.config.backend.postgres.Postgres;
import com.torodb.util.jackson.BackendDeserializer;
import com.torodb.util.jackson.BackendSerializer;

@XmlType(namespace=Config.TOROCONFIG_NAMESPACE)
@JsonSerialize(using=BackendSerializer.class)
@JsonDeserialize(using=BackendDeserializer.class)
public class Backend {
	private BackendImplementation backendImplementation = new Postgres();

	@XmlElements({
		@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE, name="postgres", type=Postgres.class, required=true),
		@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE, name="greenplum", type=Greenplum.class, required=true),
	})
	public BackendImplementation getBackendImplementation() {
		return backendImplementation;
	}
	public void setBackendImplementation(BackendImplementation backendImplementation) {
		this.backendImplementation = backendImplementation;
	}
	
	public boolean isPostgresLike() {
		return backendImplementation instanceof Postgres;
	}
	public boolean isPostgres() {
		return Postgres.class == backendImplementation.getClass();
	}
	public Postgres asPostgres() {
		assert backendImplementation instanceof Postgres;
		
		return (Postgres) backendImplementation;
	}
	public boolean isGreenplum() {
		return Greenplum.class == backendImplementation.getClass();
	}
	public Greenplum asGreenplum() {
		assert backendImplementation instanceof Greenplum;
		
		return (Greenplum) backendImplementation;
	}
}

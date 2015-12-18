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

package com.torodb.config.model.backend;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.torodb.config.jackson.BackendDeserializer;
import com.torodb.config.jackson.BackendSerializer;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.postgres.Postgres;

@JsonSerialize(using=BackendSerializer.class)
@JsonDeserialize(using=BackendDeserializer.class)
public class Backend {
	@NotNull
	@Valid
	@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.WRAPPER_OBJECT)
	@JsonSubTypes({
		@JsonSubTypes.Type(name="postgres", value=Postgres.class),
		@JsonSubTypes.Type(name="greenplum", value=Greenplum.class),
	})
	@JsonProperty(required=true)
	private BackendImplementation backendImplementation = new Postgres();

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

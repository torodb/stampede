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

package com.torodb.standalone.config.model.backend;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import com.torodb.packaging.config.model.backend.BackendImplementation;
import com.torodb.standalone.config.jackson.BackendDeserializer;
import com.torodb.standalone.config.jackson.BackendSerializer;
import com.torodb.standalone.config.model.backend.derby.Derby;
import com.torodb.standalone.config.model.backend.postgres.Postgres;

@JsonSerialize(using=BackendSerializer.class)
@JsonDeserialize(using=BackendDeserializer.class)
public class Backend extends com.torodb.packaging.config.model.backend.Backend {
    public static final ImmutableMap<String, Class<? extends BackendImplementation>> BACKEND_CLASSES =
            ImmutableMap.<String, Class<? extends BackendImplementation>>builder()
            .put("postgres", Postgres.class)
            .put("derby", Derby.class)
            .build();
    
    public Backend() {
        this(new Postgres());
    }

    public Backend(BackendImplementation backendImplementation) {
        super(BACKEND_CLASSES);
        setBackendImplementation(backendImplementation);
    }
	
    @NotNull
    @Valid
    @JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonSubTypes({
        @JsonSubTypes.Type(name="postgres", value=Postgres.class),
        @JsonSubTypes.Type(name="derby", value=Derby.class),
    })
    @JsonProperty(required=true)
	public BackendImplementation getBackendImplementation() {
		return super.getBackendImplementation();
	}
}
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

package com.torodb.config.model;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.annotation.Description;
import com.torodb.config.model.backend.Backend;
import com.torodb.config.model.generic.Generic;
import com.torodb.config.model.protocol.Protocol;


@JsonPropertyOrder({
	"generic",
	"protocol",
	"backend"
})
public class Config {
	
	@NotNull
	@Valid
	@JsonProperty(required=true)
	private Generic generic = new Generic();
	@NotNull
	@Valid
	@JsonProperty(required=true)
	private Protocol protocol = new Protocol();
	@Description("config.backend")
	@NotNull
	@Valid
	@JsonProperty(required=true)
	private Backend backend = new Backend();

	public Generic getGeneric() {
		return generic;
	}
	public void setGeneric(Generic generic) {
		this.generic = generic;
	}
	public Protocol getProtocol() {
		return protocol;
	}
	public void setProtocol(Protocol protocol) {
		this.protocol = protocol;
	}
	public Backend getBackend() {
		return backend;
	}
	public void setBackend(Backend backend) {
		this.backend = backend;
	}
}

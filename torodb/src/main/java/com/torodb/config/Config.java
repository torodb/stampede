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
package com.torodb.config;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.backend.Backend;
import com.torodb.config.generic.Generic;
import com.torodb.config.protocol.Protocol;


@XmlRootElement(name="config", namespace=Config.TOROCONFIG_NAMESPACE)
@XmlType(namespace=Config.TOROCONFIG_NAMESPACE,
propOrder={
	"generic",
	"protocol",
	"backend"
})
@JsonPropertyOrder({
	"generic",
	"protocol",
	"backend"
})
public class Config {
	
	public static final String TOROCONFIG_NAMESPACE = "http://torodb.com/config";
	
	private Generic generic = new Generic();
	private Protocol protocol = new Protocol();
	/**
	 * Backend configuration (only one can be specified)
	 */
	private Backend backend = new Backend();


	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE, required=true)
	public Generic getGeneric() {
		return generic;
	}
	public void setGeneric(Generic generic) {
		this.generic = generic;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE, required=true)
	public Protocol getProtocol() {
		return protocol;
	}
	public void setProtocol(Protocol protocol) {
		this.protocol = protocol;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE, required=true)
	public Backend getBackend() {
		return backend;
	}
	public void setBackend(Backend backend) {
		this.backend = backend;
	}
}

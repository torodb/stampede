package com.torodb.config.protocol.mongo;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.Config;

@XmlType(namespace=Config.TOROCONFIG_NAMESPACE)
@JsonPropertyOrder({
	"replSetName",
	"role"
})
public class Replication {
	
	@XmlType(namespace=Config.TOROCONFIG_NAMESPACE)
	@XmlEnum
	public enum Role {
		/**
		 * The instance will not participate in voting and can not be elected as master
		 */
		HIDDEN_SLAVE;
	}
	
	/**
	 * The name of the MongoDB Replica Set where this instance will attach
	 */
	private String replSetName;
	/**
	 * The role that this instance will assume in the replica set. When value is HIDDEN_SLAVE the instance will not participate in voting and can not be elected as master.
	 */
	private Role role = Role.HIDDEN_SLAVE;
	/**
	 * The host and port (<host>:<port>) of the node from ToroDB has to replicate. If this node must run as primary, this paramenter must not be defined
	 */
	private String syncSource;
	
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getReplSetName() {
		return replSetName;
	}
	public void setReplSetName(String replSetName) {
		this.replSetName = replSetName;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public Role getRole() {
		return role;
	}
	public void setRole(Role role) {
		this.role = role;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getSyncSource() {
		return syncSource;
	}
	public void setSyncSource(String syncSource) {
		this.syncSource = syncSource;
	}
}

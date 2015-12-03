package com.torodb.config.protocol.mongo;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.Config;

/**
 * MongoDB protocol configuration
 */
@XmlType(namespace=Config.TOROCONFIG_NAMESPACE, propOrder={})
@JsonPropertyOrder({
	"net",
	"replication"
})
public class Mongo {
	private Net net = new Net();
	private List<Replication> replication;

	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE, required=true)
	public Net getNet() {
		return net;
	}
	public void setNet(Net net) {
		this.net = net;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public List<Replication> getReplication() {
		return replication;
	}
	public void setReplication(List<Replication> replication) {
		this.replication = replication;
	}
}

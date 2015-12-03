package com.torodb.config.protocol;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import com.torodb.config.Config;
import com.torodb.config.protocol.mongo.Mongo;

@XmlType(namespace=Config.TOROCONFIG_NAMESPACE,
propOrder={})
public class Protocol {
	private Mongo mongo = new Mongo();

	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE, required=true)
	public Mongo getMongo() {
		return mongo;
	}
	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}
}

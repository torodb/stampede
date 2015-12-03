package com.torodb.config.protocol.mongo;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.Config;

@XmlType(namespace=Config.TOROCONFIG_NAMESPACE)
@JsonPropertyOrder({
	"bindIp",
	"port"
})
public class Net {
	/**
	 * The host or IP associate to the interface where clients will connect to. Use * to specify any interface
	 */
    private String bindIp = "localhost";
    /**
     * The port where the clients will connect to
     */
    private int port = 27018;

	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
	public String getBindIp() {
		return bindIp;
	}
	public void setBindIp(String bindIp) {
		this.bindIp = bindIp;
	}
	@XmlElement(namespace=Config.TOROCONFIG_NAMESPACE)
    public int getPort() {
        return port;
    }
    public void setPort(int port) {
        this.port = port;
    }
}

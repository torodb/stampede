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

package com.torodb.config.model.protocol.mongo;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.annotation.Description;
import com.torodb.config.validation.NoDuplicatedReplName;

@Description("config.protocol.mongo")
@JsonPropertyOrder({
	"net",
	"replication"
})
public class Mongo {
	@NotNull
	@Valid
	@JsonProperty(required=true)
	private Net net = new Net();
	@Valid
	@NoDuplicatedReplName
	private List<Replication> replication;
    @Description("config.protocol.mongo.cursorTimeout")
    @NotNull
    @JsonProperty(required=true)
    private Long cursorTimeout = 10L * 60 * 1000;

	public Net getNet() {
		return net;
	}
	public void setNet(Net net) {
		this.net = net;
	}
	public List<Replication> getReplication() {
		return replication;
	}
	public void setReplication(List<Replication> replication) {
		this.replication = replication;
	}
    public Long getCursorTimeout() {
        return cursorTimeout;
    }
    public void setCursorTimeout(Long cursorTimeout) {
        this.cursorTimeout = cursorTimeout;
    }
}

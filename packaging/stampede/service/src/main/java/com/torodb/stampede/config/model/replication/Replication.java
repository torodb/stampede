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

package com.torodb.stampede.config.model.replication;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.backend.CursorConfig;
import com.torodb.packaging.config.util.ConfigUtils;

@JsonPropertyOrder({
    "net",
    "replication",
    "cursorTimeout",
    "mongopassFile"
})
public class Replication extends com.torodb.packaging.config.model.protocol.mongo.Replication implements CursorConfig {
    @Description("config.mongo.cursorTimeout")
    @NotNull
    @JsonProperty(required=true)
    private Long cursorTimeout = 10L * 60 * 1000;
    @Description("config.mongo.mongopassFile")
    @JsonProperty(required=true)
    private String mongopassFile = ConfigUtils.getUserHomeFilePath(".mongopass");

    public Replication() {
        setSyncSource("localhost:27017");
        setReplSetName("rs1");
    }
    
    @Override
    public Long getCursorTimeout() {
        return cursorTimeout;
    }
    public void setCursorTimeout(Long cursorTimeout) {
        this.cursorTimeout = cursorTimeout;
    }
    public String getMongopassFile() {
        return mongopassFile;
    }
    public void setMongopassFile(String mongopassFile) {
        this.mongopassFile = mongopassFile;
    }
}

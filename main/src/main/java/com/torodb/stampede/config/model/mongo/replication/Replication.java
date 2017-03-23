/*
 * ToroDB Stampede
 * Copyright © 2016 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.stampede.config.model.mongo.replication;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.common.EnumWithDefault;
import com.torodb.packaging.config.model.common.StringWithDefault;
import com.torodb.packaging.config.model.protocol.mongo.AbstractReplication;
import com.torodb.packaging.config.model.protocol.mongo.AbstractShardReplication;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.config.validation.NotEmptySrtingWithDefault;

import java.util.List;

import javax.validation.Valid;

@JsonPropertyOrder({"replSetName", "syncSource", "ssl", "auth", "include", "exclude",
    "mongopassFile", "shards"})
public class Replication extends AbstractReplication<ShardReplication> {

  private String mongopassFile = ConfigUtils.getUserHomeFilePath(".mongopass");

  public Replication() {
    super.setSyncSource(StringWithDefault.withDefault("localhost:27017"));
    super.setReplSetName(StringWithDefault.withDefault("rs1"));
  }

  @JsonIgnore
  public StringWithDefault getName() {
    return super.getName();
  }
  
  @Description("config.mongo.replication.replSetName")
  @NotEmptySrtingWithDefault
  @JsonProperty(required = false)
  public StringWithDefault getReplSetName() {
    return super.getReplSetName();
  }

  @JsonIgnore
  public EnumWithDefault<Role> getRole() {
    return super.getRole();
  }

  @Description("config.mongo.replication.syncSource")
  @NotEmptySrtingWithDefault
  @JsonProperty(required = false)
  public StringWithDefault getSyncSource() {
    return super.getSyncSource();
  }
  
  @Description("config.mongo.mongopassFile")
  @JsonProperty(required = true)
  public String getMongopassFile() {
    return mongopassFile;
  }

  /**
   * This method replicates #getShardList, but it is needed to avoid errors on jackson, please
   * use that method instead.
   *
   * @see #getShardList() 
   */
  @Description("config.mongo.shards")
  @Valid
  @JsonProperty(required = false)
  public List<ShardReplication> getShards() {
    return super.getShardList();
  }

  public void setShards(List<ShardReplication> shards) {
    super.setShardList(shards);
  }

  public void setMongopassFile(String mongopassFile) {
    this.mongopassFile = mongopassFile;
  }

  @Override
  public void setReplSetName(StringWithDefault replSetName) {
    super.setReplSetName(replSetName);
  }

  @Override
  public void setSyncSource(StringWithDefault syncSource) {
    super.setSyncSource(syncSource);
  }
  
  public ShardReplication mergeWith(AbstractShardReplication shard) {
    ShardReplication mergedShard = new ShardReplication();
    
    merge(shard, mergedShard);
    
    return mergedShard;
  }
}

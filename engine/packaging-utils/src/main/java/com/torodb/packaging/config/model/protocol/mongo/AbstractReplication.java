/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
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

package com.torodb.packaging.config.model.protocol.mongo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.common.ScalarWithDefault;

import java.util.ArrayList;
import java.util.List;

@JsonPropertyOrder({"replSetName", "role", "syncSource", 
    "ssl", "auth", "include", "exclude", "shards"})
public abstract class AbstractReplication<T extends AbstractShardReplication> 
    extends AbstractShardReplication {

  private FilterList include;
  private FilterList exclude;
  private List<T> shards = new ArrayList<>();

  @Description("config.mongo.replication.include")
  @JsonProperty(required = true)
  public FilterList getInclude() {
    return include;
  }

  public void setInclude(FilterList include) {
    this.include = include;
  }

  @Description("config.mongo.replication.exclude")
  @JsonProperty(required = true)
  public FilterList getExclude() {
    return exclude;
  }

  public void setExclude(FilterList exclude) {
    this.exclude = exclude;
  }

  @JsonIgnore
  public List<T> getShardList() {
    return shards;
  }

  public void setShardList(List<T> shards) {
    this.shards = shards;
  }

  @JsonIgnore
  public boolean isShardingReplication() {
    return !getShardList().isEmpty();
  }

  protected void merge(AbstractShardReplication shard, AbstractShardReplication mergedShard) {
    mergedShard.setReplSetName(ScalarWithDefault.merge(shard.getReplSetName(), getReplSetName()));
    mergedShard.setSyncSource(shard.getSyncSource());
    
    Auth mergedAuth = new Auth();
    mergedAuth.setMode(ScalarWithDefault.merge(
        shard.getAuth().getMode(), getAuth().getMode()));
    mergedAuth.setSource(ScalarWithDefault.merge(
        shard.getAuth().getSource(), getAuth().getSource()));
    mergedAuth.setUser(ScalarWithDefault.merge(
        shard.getAuth().getUser(), getAuth().getUser()));
    mergedShard.setAuth(mergedAuth);
    
    Ssl mergedSsl = new Ssl();
    mergedSsl.setAllowInvalidHostnames(ScalarWithDefault.merge(
        shard.getSsl().getAllowInvalidHostnames(), getSsl().getAllowInvalidHostnames()));
    mergedSsl.setCaFile(ScalarWithDefault.merge(
        shard.getSsl().getCaFile(), getSsl().getCaFile()));
    mergedSsl.setEnabled(ScalarWithDefault.merge(
        shard.getSsl().getEnabled(), getSsl().getEnabled()));
    mergedSsl.setFipsMode(ScalarWithDefault.merge(
        shard.getSsl().getFipsMode(), getSsl().getFipsMode()));
    mergedSsl.setKeyPassword(ScalarWithDefault.merge(
        shard.getSsl().getKeyPassword(), getSsl().getKeyPassword()));
    mergedSsl.setKeyStoreFile(ScalarWithDefault.merge(
        shard.getSsl().getKeyStoreFile(), getSsl().getKeyStoreFile()));
    mergedSsl.setKeyStorePassword(ScalarWithDefault.merge(
        shard.getSsl().getKeyStorePassword(), getSsl().getKeyStorePassword()));
    mergedSsl.setTrustStoreFile(ScalarWithDefault.merge(
        shard.getSsl().getTrustStoreFile(), getSsl().getTrustStoreFile()));
    mergedSsl.setTrustStorePassword(ScalarWithDefault.merge(
        shard.getSsl().getTrustStorePassword(), getSsl().getTrustStorePassword()));
    mergedShard.setSsl(mergedSsl);
  }
}

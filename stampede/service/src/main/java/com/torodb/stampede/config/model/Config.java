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

package com.torodb.stampede.config.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.core.metrics.MetricsConfig;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.stampede.config.model.backend.Backend;
import com.torodb.stampede.config.model.logging.Logging;
import com.torodb.stampede.config.model.replication.Replication;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"logging", "metricsEnabled", "replication", "backend"})
public class Config implements MetricsConfig {

  @Description("config.logging")
  @NotNull
  @Valid
  @JsonProperty(required = true)
  private Logging logging = new Logging();
  @Description("config.generic.metricsEnabled")
  @NotNull
  @JsonProperty(required = true)
  private Boolean metricsEnabled = false;
  @NotNull
  @Valid
  @JsonProperty(required = true)
  private Replication replication = new Replication();
  @Description("config.backend")
  @NotNull
  @Valid
  @JsonProperty(required = true)
  private Backend backend = new Backend();

  public Logging getLogging() {
    return logging;
  }

  public void setLogging(Logging logging) {
    if (logging != null) {
      this.logging = logging;
    }
  }

  @Override
  public Boolean getMetricsEnabled() {
    return metricsEnabled;
  }

  public void setMetricsEnabled(Boolean metricsEnabled) {
    this.metricsEnabled = metricsEnabled;
  }

  public Replication getReplication() {
    return replication;
  }

  public void setReplication(Replication replication) {
    this.replication = replication;
  }

  public Backend getBackend() {
    return backend;
  }

  public void setBackend(Backend backend) {
    this.backend = backend;
  }
}

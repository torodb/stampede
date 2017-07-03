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
package com.torodb.stampede.config.model.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.mongodb.repl.oplogreplier.offheapbuffer.BufferRollCycle;
import com.torodb.mongodb.repl.oplogreplier.offheapbuffer.OffHeapBufferConfig;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.util.ConfigUtils;

@Description("config.offHeapBuffer")
@JsonPropertyOrder({"enabled", "path", "rollCycle", "maxFiles"})
public class OffHeapBuffer implements OffHeapBufferConfig {

  @Description("config.offHeapBuffer.enabled")
  @JsonProperty(required = true)
  private Boolean enabled;

  @Description("config.offHeapBuffer.path")
  private String path;

  @Description("config.offHeapBuffer.rollcycle")
  private BufferRollCycle rollCycle;

  @Description("config.offHeapBuffer.maxFiles")
  private int maxFiles;

  public OffHeapBuffer() {
    enabled = false;
    path = ConfigUtils.getDefaultTempPath();
    rollCycle = BufferRollCycle.DAILY;
    maxFiles = 5;
  }

  @Override
  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public int getMaxFiles() {
    return maxFiles;
  }

  public void setMaxFiles(int maxFiles) {
    this.maxFiles = maxFiles;
  }

  @Override
  public BufferRollCycle getRollCycle() {
    return rollCycle;
  }

  public void setRollCycle(BufferRollCycle rollCycle) {
    this.rollCycle = rollCycle;
  }
}

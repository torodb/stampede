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

package com.torodb.packaging.config.model.backend.postgres;

import com.torodb.packaging.config.model.backend.BackendImplementation;
import com.torodb.packaging.config.model.backend.BackendPasswordConfig;
import com.torodb.packaging.config.visitor.BackendImplementationVisitor;

public abstract class AbstractPostgres implements BackendImplementation, BackendPasswordConfig {

  private String host;
  private Integer port;
  private String database;
  private String user;
  private String password;
  private String toropassFile;
  private String applicationName;
  private Boolean includeForeignKeys;

  protected AbstractPostgres(String host, Integer port, String database, String user,
      String password, String toropassFile,
      String applicationName, Boolean includeForeignKeys) {
    super();
    this.host = host;
    this.port = port;
    this.database = database;
    this.user = user;
    this.password = password;
    this.toropassFile = toropassFile;
    this.applicationName = applicationName;
    this.includeForeignKeys = includeForeignKeys;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public String getToropassFile() {
    return toropassFile;
  }

  public void setToropassFile(String toropassFile) {
    this.toropassFile = toropassFile;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public void setIncludeForeignKeys(Boolean includeForeignKeys) {
    this.includeForeignKeys = includeForeignKeys;
  }

  public Boolean getIncludeForeignKeys() {
    return includeForeignKeys;
  }

  @Override
  public void accept(BackendImplementationVisitor visitor) {
    visitor.visit(this);
  }
}

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

package com.torodb.backend;

public class SqlBuilder {

  private final StringBuilder sb;

  public SqlBuilder(String init) {
    this.sb = new StringBuilder();
    this.sb.append(init);
  }

  public SqlBuilder(StringBuilder sb) {
    this.sb = sb;
  }

  public SqlBuilder append(String str) {
    sb.append(str);
    return this;
  }

  public SqlBuilder append(char c) {
    sb.append(c);
    return this;
  }

  public SqlBuilder quote(String str) {
    sb.append('"').append(str).append('"');
    return this;
  }

  public SqlBuilder quote(Enum<?> enumValue) {
    sb.append('"').append(enumValue.toString()).append('"');
    return this;
  }

  public SqlBuilder table(String schema, String table) {
    sb.append('"').append(schema).append("\".\"").append(table).append('"');
    return this;
  }

  public SqlBuilder setLastChar(char c) {
    sb.setCharAt(sb.length() - 1, c);
    return this;
  }

  public SqlBuilder setCharAt(int index, char c) {
    sb.setCharAt(index, c);
    return this;
  }

  public int length() {
    return sb.length();
  }

  public String toString() {
    return sb.toString();
  }
}

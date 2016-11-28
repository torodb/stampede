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

package com.torodb.packaging.config.model.generic;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.torodb.packaging.config.jackson.LogPackagesDeserializer;
import com.torodb.packaging.config.jackson.LogPackagesSerializer;

import java.util.HashMap;

@JsonSerialize(using = LogPackagesSerializer.class)
@JsonDeserialize(using = LogPackagesDeserializer.class)
public class LogPackages extends HashMap<String, LogLevel> {

  private static final long serialVersionUID = -167180938971456194L;
}

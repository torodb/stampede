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

package com.torodb.packaging.config.model.common;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.torodb.packaging.config.jackson.IntegerWithDefaultDeserializer;
import com.torodb.packaging.config.jackson.ScalarWithDefaultSerializer;

@JsonSerialize(using = ScalarWithDefaultSerializer.class)
@JsonDeserialize(using = IntegerWithDefaultDeserializer.class)
public class IntegerWithDefault extends ScalarWithDefault<Integer> {

  public IntegerWithDefault(Integer value, boolean isDefault) {
    super(value, isDefault);
  }

  public static IntegerWithDefault withDefault(Integer defaultValue) {
    return new IntegerWithDefault(defaultValue, true);
  }
}

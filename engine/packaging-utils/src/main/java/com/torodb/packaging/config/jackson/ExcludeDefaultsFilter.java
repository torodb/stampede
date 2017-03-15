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

package com.torodb.packaging.config.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.torodb.packaging.config.model.common.ScalarWithDefault;

public class ExcludeDefaultsFilter extends SimpleBeanPropertyFilter {

  @Override
  protected boolean include(BeanPropertyWriter writer) {
    return true;
  }

  @Override
  protected boolean include(PropertyWriter writer) {
    return true;
  }

  @Override
  public void serializeAsField(Object pojo, JsonGenerator jgen, SerializerProvider provider,
      PropertyWriter writer) throws Exception {
    if (pojo instanceof ScalarWithDefault && ((ScalarWithDefault<?>) pojo).isDefault()) {
      writer.serializeAsOmittedField(pojo, jgen, provider);
    } else {
      super.serializeAsField(pojo, jgen, provider, writer);
    }
  }
}

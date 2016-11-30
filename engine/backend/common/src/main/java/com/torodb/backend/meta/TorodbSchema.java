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

package com.torodb.backend.meta;

import org.jooq.Table;
import org.jooq.impl.SchemaImpl;

import java.util.List;

public class TorodbSchema extends SchemaImpl {

  private static final long serialVersionUID = -1813122131;

  public static final String IDENTIFIER = "torodb";

  /**
   * The reference instance of <code>torodb</code>
   */
  public static final TorodbSchema TORODB = new TorodbSchema();

  /**
   * No further instances allowed
   */
  protected TorodbSchema() {
    super(IDENTIFIER);
  }

  @Override
  public final List<Table<?>> getTables() {
    throw new UnsupportedOperationException();
  }
}

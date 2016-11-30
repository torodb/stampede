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

package com.torodb.backend.derby;

import com.google.common.collect.ImmutableSet;
import com.torodb.backend.AbstractIdentifierConstraints;

import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class DerbyIdentifierConstraints extends AbstractIdentifierConstraints {

  public DerbyIdentifierConstraints() {
    super(
        ImmutableSet.<String>of(
            "NULLID",
            "SQLJ",
            "SYS",
            "SYSCAT",
            "SYSCS_DIAG",
            "SYSCS_UTIL",
            "SYSFUN",
            "SYSIBM",
            "SYSPROC",
            "SYSSTAT"
        ),
        ImmutableSet.<String>of());
  }

  @Override
  public int identifierMaxSize() {
    return 128;
  }
}

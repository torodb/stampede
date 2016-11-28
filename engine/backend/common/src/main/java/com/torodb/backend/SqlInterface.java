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

import com.torodb.core.backend.IdentifierConstraints;

import javax.annotation.Nonnull;

/**
 * Wrapper interface to define all database-specific SQL code
 */
public interface SqlInterface {

  @Nonnull
  MetaDataReadInterface getMetaDataReadInterface();

  @Nonnull
  MetaDataWriteInterface getMetaDataWriteInterface();

  @Nonnull
  DataTypeProvider getDataTypeProvider();

  @Nonnull
  StructureInterface getStructureInterface();

  @Nonnull
  ReadInterface getReadInterface();

  @Nonnull
  WriteInterface getWriteInterface();

  @Nonnull
  IdentifierConstraints getIdentifierConstraints();

  @Nonnull
  ErrorHandler getErrorHandler();

  @Nonnull
  DslContextFactory getDslContextFactory();

  @Nonnull
  DbBackendService getDbBackend();

}

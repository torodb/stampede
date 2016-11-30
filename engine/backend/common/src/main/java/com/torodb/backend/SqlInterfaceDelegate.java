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

import javax.inject.Inject;

public class SqlInterfaceDelegate implements SqlInterface {

  private final MetaDataReadInterface metaDataReadInterface;
  private final MetaDataWriteInterface metaDataWriteInterface;
  private final DataTypeProvider dataTypeProvider;
  private final StructureInterface structureInterface;
  private final ReadInterface readInterface;
  private final WriteInterface writeInterface;
  private final IdentifierConstraints identifierConstraints;
  private final ErrorHandler errorHandler;
  private final DslContextFactory dslContextFactory;
  private final DbBackendService dbBackend;

  @Inject
  public SqlInterfaceDelegate(MetaDataReadInterface metaDataReadInterface,
      MetaDataWriteInterface metaDataWriteInterface, DataTypeProvider dataTypeProvider,
      StructureInterface structureInterface, ReadInterface readInterface,
      WriteInterface writeInterface,
      IdentifierConstraints identifierConstraints, ErrorHandler errorHandler,
      DslContextFactory dslContextFactory, DbBackendService dbBackend) {
    super();
    this.metaDataReadInterface = metaDataReadInterface;
    this.metaDataWriteInterface = metaDataWriteInterface;
    this.dataTypeProvider = dataTypeProvider;
    this.structureInterface = structureInterface;
    this.readInterface = readInterface;
    this.writeInterface = writeInterface;
    this.identifierConstraints = identifierConstraints;
    this.errorHandler = errorHandler;
    this.dslContextFactory = dslContextFactory;
    this.dbBackend = dbBackend;
  }

  public MetaDataReadInterface getMetaDataReadInterface() {
    return metaDataReadInterface;
  }

  public MetaDataWriteInterface getMetaDataWriteInterface() {
    return metaDataWriteInterface;
  }

  public DataTypeProvider getDataTypeProvider() {
    return dataTypeProvider;
  }

  public StructureInterface getStructureInterface() {
    return structureInterface;
  }

  public ReadInterface getReadInterface() {
    return readInterface;
  }

  public WriteInterface getWriteInterface() {
    return writeInterface;
  }

  public IdentifierConstraints getIdentifierConstraints() {
    return identifierConstraints;
  }

  public ErrorHandler getErrorHandler() {
    return errorHandler;
  }

  public DslContextFactory getDslContextFactory() {
    return dslContextFactory;
  }

  public DbBackendService getDbBackend() {
    return dbBackend;
  }
}

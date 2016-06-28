/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.backend;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.index.NamedDbIndex;
import com.torodb.backend.tables.*;
import com.torodb.backend.tables.records.*;
import com.torodb.core.TableRef;
import com.torodb.core.backend.DidCursor;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;

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
    private final DbBackend dbBackend;
    
    @Inject
    public SqlInterfaceDelegate(MetaDataReadInterface metaDataReadInterface,
            MetaDataWriteInterface metaDataWriteInterface, DataTypeProvider dataTypeProvider,
            StructureInterface structureInterface, ReadInterface readInterface, WriteInterface writeInterface,
            IdentifierConstraints identifierConstraints, ErrorHandler errorHandler,
            DslContextFactory dslContextFactory, DbBackend dbBackend) {
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

    public DbBackend getDbBackend() {
        return dbBackend;
    }
}

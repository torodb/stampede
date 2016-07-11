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

package com.torodb.backend.derby;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.torodb.backend.AbstractMetaDataReadInterface;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.derby.tables.DerbyMetaCollectionTable;
import com.torodb.backend.derby.tables.DerbyMetaDatabaseTable;
import com.torodb.backend.derby.tables.DerbyMetaDocPartTable;
import com.torodb.backend.derby.tables.DerbyMetaFieldTable;
import com.torodb.backend.derby.tables.DerbyMetaScalarTable;
import com.torodb.backend.index.NamedDbIndex;

/**
 *
 */
@Singleton
public class DerbyMetaDataReadInterface extends AbstractMetaDataReadInterface {

    private final DerbyMetaDatabaseTable metaDatabaseTable;
    private final DerbyMetaCollectionTable metaCollectionTable;
    private final DerbyMetaDocPartTable metaDocPartTable;
    private final DerbyMetaFieldTable metaFieldTable;
    private final DerbyMetaScalarTable metaScalarTable;

    @Inject
    public DerbyMetaDataReadInterface(SqlHelper sqlHelper) {
        super(DerbyMetaDocPartTable.DOC_PART, sqlHelper);
        
        this.metaDatabaseTable = DerbyMetaDatabaseTable.DATABASE;
        this.metaCollectionTable = DerbyMetaCollectionTable.COLLECTION;
        this.metaDocPartTable = DerbyMetaDocPartTable.DOC_PART;
        this.metaFieldTable = DerbyMetaFieldTable.FIELD;
        this.metaScalarTable = DerbyMetaScalarTable.SCALAR;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public DerbyMetaDatabaseTable getMetaDatabaseTable() {
        return metaDatabaseTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public DerbyMetaCollectionTable getMetaCollectionTable() {
        return metaCollectionTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public DerbyMetaDocPartTable getMetaDocPartTable() {
        return metaDocPartTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public DerbyMetaFieldTable getMetaFieldTable() {
        return metaFieldTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public DerbyMetaScalarTable getMetaScalarTable() {
        return metaScalarTable;
    }

    @Override
    protected String getReadDatabaseSizeStatement(String databaseName) {
        //TODO: This throw a ERROR XJ001: Java exception: ': java.lang.NullPointerException'.
//        return "SELECT sum((NUMALLOCATEDPAGES + NUMFREEPAGES) * PAGESIZE) FROM"
//                + " ("
//                + "SELECT SPACE_TABLE.*"
//                + " FROM SYS.SYSTABLES SYSTABLES,"
//                + " SYS.SYSSCHEMAS SYSSCHEMAS,"
//                + " TABLE (SYSCS_DIAG.SPACE_TABLE(SCHEMANAME, TABLENAME)) AS SPACE_TABLE"
//                + " WHERE SYSTABLES.SCHEMAID = SYSSCHEMAS.SCEHMAID"
//                + " AND TABLETYPE = 'T'"
//                + " AND SCEHMANAME = ?"
//                + ") SPACE_TABLE";
        return "SELECT 0 FROM SYSIBM.SYSDUMMY1 WHERE ? IS NOT NULL";
    }

    @Override
    protected String getReadCollectionSizeStatement(String schema, String collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getReadDocumentsSizeStatement(String schema, String collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getReadIndexSizeStatement(String schema, String collection, String index,
            Set<NamedDbIndex> relatedDbIndexes, Map<String, Integer> relatedToroIndexes) {
        throw new UnsupportedOperationException();
    }
}

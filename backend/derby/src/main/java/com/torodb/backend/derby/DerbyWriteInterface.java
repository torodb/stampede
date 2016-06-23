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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.torodb.backend.AbstractWriteInterface;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;

/**
 *
 */
@Singleton
public class DerbyWriteInterface extends AbstractWriteInterface {
    
    @Inject
    public DerbyWriteInterface(DerbyMetaDataReadInterface derbyMetaDataReadInterface,
            DerbyDataTypeProvider derbyDataTypeProvider, ErrorHandler errorHandler,
            SqlHelper sqlHelper) {
        super(derbyMetaDataReadInterface, derbyDataTypeProvider, errorHandler, sqlHelper);
    }

    @Override
    protected String getDeleteDocPartsStatement(String schemaName, String tableName, List<Integer> dids) {
        StringBuilder sb = new StringBuilder()
                .append("DELETE FROM \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" WHERE \"")
                .append(MetaDocPartTable.DocPartTableFields.DID.fieldName)
                .append("\" IN (");
        for (Integer did : dids) {
            sb.append(did)
                .append(',');
        }
        sb.setCharAt(sb.length() - 1, ')');
        String statement = sb.toString();
        return statement;
    }

    @Override
    protected String getInsertDocPartDataStatement(String schemaName, MetaDocPart metaDocPart,
            Iterator<MetaField> metaFieldIterator, Iterator<MetaScalar> metaScalarIterator,
            Collection<InternalField<?>> internalFields, List<FieldType> fieldTypeList) {
        final StringBuilder insertStatementBuilder = new StringBuilder(2048);
        final StringBuilder insertStatementValuesBuilder = new StringBuilder(1024);
        insertStatementBuilder.append("INSERT INTO \"")
            .append(schemaName)
            .append("\".\"")
            .append(metaDocPart.getIdentifier())
            .append("\" (");
        insertStatementValuesBuilder.append(" VALUES (");
        for (InternalField<?> internalField : internalFields) {
            insertStatementBuilder.append("\"")
                .append(internalField.getName())
                .append("\",");
            insertStatementValuesBuilder.append("?,");
        }
        while (metaScalarIterator.hasNext()) {
            MetaScalar metaScalar = metaScalarIterator.next();
            insertStatementBuilder.append("\"")
                .append(metaScalar.getIdentifier())
                .append("\",");
            insertStatementValuesBuilder.append("?,");
            fieldTypeList.add(metaScalar.getType());
        }
        while (metaFieldIterator.hasNext()) {
            MetaField metaField = metaFieldIterator.next();
            insertStatementBuilder.append("\"")
                .append(metaField.getIdentifier())
                .append("\",");
            insertStatementValuesBuilder.append("?,");
            fieldTypeList.add(metaField.getType());
        }
        insertStatementBuilder.setCharAt(insertStatementBuilder.length() - 1, ')');
        insertStatementValuesBuilder.setCharAt(insertStatementValuesBuilder.length() - 1, ')');
        insertStatementBuilder.append(insertStatementValuesBuilder);
        
        String statement = insertStatementBuilder.toString();
        return statement;
    }
}
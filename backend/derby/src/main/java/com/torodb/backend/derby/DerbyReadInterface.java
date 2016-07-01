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

import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.Converter;

import com.torodb.backend.AbstractReadInterface;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.tables.MetaDocPartTable.DocPartTableFields;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;

/**
 *
 */
@Singleton
public class DerbyReadInterface extends AbstractReadInterface {

    private final DerbyMetaDataReadInterface metaDataReadInterface;
    
    @Inject
    public DerbyReadInterface(DerbyMetaDataReadInterface metaDataReadInterface, DerbyDataTypeProvider dataTypeProvider, 
            DerbyErrorHandler errorhandler, SqlHelper sqlHelper, TableRefFactory tableRefFactory) {
        super(metaDataReadInterface, dataTypeProvider, errorhandler, sqlHelper, tableRefFactory);
        this.metaDataReadInterface = metaDataReadInterface;
    }

    @Override
    protected String getReadCollectionDidsWithFieldEqualsToStatement(String schemaName, String rootTableName,
            String columnName) {
        StringBuilder sb = new StringBuilder()
                .append("SELECT \"")
                .append(DocPartTableFields.DID.fieldName)
                .append("\" FROM \"")
                .append(schemaName)
                .append("\".\"")
                .append(rootTableName)
                .append("\" WHERE \"")
                .append(schemaName)
                .append("\".\"")
                .append(rootTableName)
                .append("\".\"")
                .append(columnName)
                .append("\" = ? GROUP BY \"")
                .append(DocPartTableFields.DID.fieldName)
                .append("\" ORDER BY \"")
                .append(DocPartTableFields.DID.fieldName)
                .append('"');
        String statement = sb.toString();
        return statement;
    }

    @Override
    protected String getReadAllCollectionDidsStatement(String schemaName, String rootTableName) {
        StringBuilder sb = new StringBuilder()
                .append("SELECT \"")
                .append(DocPartTableFields.DID.fieldName)
                .append("\" FROM \"")
                .append(schemaName)
                .append("\".\"")
                .append(rootTableName)
                .append('"');
        String statement = sb.toString();
        return statement;
    }

    @Override
    protected String getDocPartStatament(MetaDatabase metaDatabase, MetaDocPart metaDocPart,
            Collection<Integer> dids) {
        StringBuilder sb = new StringBuilder()
                .append("SELECT ");
        Collection<InternalField<?>> internalFields = metaDataReadInterface.getInternalFields(metaDocPart);
        for (InternalField<?> internalField : internalFields) {
            sb.append('"')
                .append(internalField.getName())
                .append("\",");
        }
        metaDocPart.streamScalars().forEach(metaScalar -> {
            sb.append('"')
                .append(metaScalar.getIdentifier())
                .append("\",");
        });
        metaDocPart.streamFields().forEach(metaField -> {
            sb.append('"')
                .append(metaField.getIdentifier())
                .append("\",");
        });
        sb.setCharAt(sb.length() - 1, ' ');
        sb
            .append("FROM \"")
            .append(metaDatabase.getIdentifier())
            .append("\".\"")
            .append(metaDocPart.getIdentifier())
            .append("\" WHERE \"")
            .append(metaDataReadInterface.getMetaDocPartTable().DID.getName())
            .append("\" IN (");
        Converter<?, Integer> converter = 
                metaDataReadInterface.getMetaDocPartTable().DID.getDataType().getConverter();
        for (Integer requestedDoc : dids) {
            sb.append(converter.to(requestedDoc))
                .append(',');
        }
        sb.setCharAt(sb.length() - 1, ')');
        if (!metaDocPart.getTableRef().isRoot()) {
            sb.append(" ORDER BY ");
            for (InternalField<?> internalField : internalFields) {
                sb
                    .append('"')
                    .append(internalField.getName())
                    .append("\",");
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    protected String getLastRowIdUsedStatement(MetaDatabase metaDatabase, MetaDocPart metaDocPart) {
        TableRef tableRef = metaDocPart.getTableRef();
        
        StringBuilder sb = new StringBuilder();
		sb.append("SELECT max(\"")
		  .append(getPrimaryKeyColumnIdentifier(tableRef))
		  .append("\") FROM \"")
		  .append(metaDatabase.getIdentifier())
		  .append("\".\"")
		  .append(metaDocPart.getIdentifier())
		  .append("\"");
		String statement = sb.toString();
        return statement;
    }
}

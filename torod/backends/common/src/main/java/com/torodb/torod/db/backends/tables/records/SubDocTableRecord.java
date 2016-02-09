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

package com.torodb.torod.db.backends.tables.records;

import com.torodb.torod.core.subdocument.SimpleSubDocTypeBuilderProvider;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.db.backends.tables.SubDocHelper;
import com.torodb.torod.db.backends.tables.SubDocTable;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;
import javax.inject.Provider;
import org.jooq.Field;
import org.jooq.impl.TableRecordImpl;

/**
 *
 */
public class SubDocTableRecord extends TableRecordImpl<SubDocTableRecord> {

    private static final long serialVersionUID = -556457916;

    private final SubDocTable table;
    private transient Provider<SubDocType.Builder> subDocTypeBuilderProvider;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------
    /**
     * Create a detached RootRecord
     * <p>
     * @param table
     */
    public SubDocTableRecord(SubDocTable table, Provider<SubDocType.Builder> subDocTypeBuilderProvider) {
        super(table);
        this.table = table;
        this.subDocTypeBuilderProvider = subDocTypeBuilderProvider;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        //TODO: Try to make this class non-serializable!
        stream.defaultReadObject();
        subDocTypeBuilderProvider = new SimpleSubDocTypeBuilderProvider();
    }

    public void setDid(Integer value) {
        setValue(0, value);
    }

    public Integer getDid() {
        return (Integer) getValue(0);
    }

    public void setIndex(Integer value) {
        setValue(1, value);
    }

    @Nullable
    public Integer getIndex() {
        return (Integer) getValue(1);
    }

    public void setSubDoc(SubDocument subdoc) throws IllegalArgumentException {

        if (!table.getSubDocType().equals(subdoc.getType())) {
            throw new IllegalArgumentException("Type of table " + table + " is " + table.getSubDocType() + " which is "
                    + "different than the type of the given subdocument (" + subdoc.getType() + ")");
        }

        for (String key : subdoc.getType().getAttributeKeys()) {
            String fieldName = table.subDocHelper().toColumnName(key);

            //@gortiz: I think we can not use types here!
            Field f = field(fieldName);
            ScalarValue<?> v = subdoc.getValue(key);

            setValue(f, v);
        }
    }

    public SubDocument getSubDoc() {
        SubDocType subDocType = table.getSubDocType();
        SubDocument.Builder builder = SubDocument.Builder.withKnownType(subDocType);

        for (Field<? extends ScalarValue<? extends Serializable>> field : table.getSubDocFields()) {
            String attName = SubDocHelper.toAttributeName(field.getName());

            SubDocAttribute att = subDocType.getAttribute(attName);

            builder.add(att, getValue(field));
        }
        builder.setDocumentId(getValue(table.getDidColumn()));
        Integer index = getValue(table.getIndexColumn());
        if (index == null) {
            index = 0;
        }
        builder.setIndex(index);
        return builder.build();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

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

import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.values.Value;
import com.torodb.torod.db.backends.tables.SubDocHelper;
import com.torodb.torod.db.backends.tables.SubDocTable;
import org.jooq.Field;
import org.jooq.impl.TableRecordImpl;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public class SubDocTableRecord extends TableRecordImpl<SubDocTableRecord> {

    private static final long serialVersionUID = -556457916;

    private final SubDocTable table;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------
    /**
     * Create a detached RootRecord
     * <p>
     * @param table
     */
    public SubDocTableRecord(SubDocTable table) {
        super(table);
        this.table = table;
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

        for (Map.Entry<String, ? extends SubDocAttribute> entry : subdoc.getAttributes().entrySet()) {
            String fieldName = table.subDocHelper().toColumnName(entry.getKey());

            //@gortiz: I think we can not use types here!
            Field f = field(fieldName);
            Value v = subdoc.getValue(entry.getKey());

            setValue(f, v);
        }
    }

    public SubDocument getSubDoc() {
        SubDocument.Builder builder = new SubDocument.Builder();

        SubDocType subDocType = table.getSubDocType();

        for (Field<? extends Value<? extends Serializable>> field : table.getSubDocFields()) {
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

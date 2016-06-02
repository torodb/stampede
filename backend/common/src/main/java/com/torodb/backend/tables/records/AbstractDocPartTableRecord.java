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

package com.torodb.backend.tables.records;

import java.io.IOException;
import java.util.Iterator;

import org.jooq.Field;
import org.jooq.impl.TableRecordImpl;

import com.torodb.backend.tables.AbstractDocPartTable;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
public class AbstractDocPartTableRecord<Record extends AbstractDocPartTableRecord<Record>> extends TableRecordImpl<Record> {

    private static final long serialVersionUID = 1;

    private final AbstractDocPartTable<Record> table;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------
    /**
     * Create a detached RootRecord
     * <p>
     * @param table
     */
    public AbstractDocPartTableRecord(AbstractDocPartTable<Record> table) {
        super(table);
        this.table = table;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        //TODO: Try to make this class non-serializable!
        stream.defaultReadObject();
    }

    public void setDid(Integer value) {
        set(0, value);
    }

    public Integer getDid() {
        return (Integer) getValue(0);
    }

    public void setPathDoc(DocPartRow docPartRow) throws IllegalArgumentException {
        DocPartData docPartData = docPartRow.getDocPartData();
        if (!table.getTableRef().equals(docPartData.getMetaDocPart().getTableRef())) {
            throw new IllegalArgumentException("Path of table " + table + " is " + table.getTableRef() + " which is "
                    + "different than the type of the given subdocument (" + docPartData.getMetaDocPart().getTableRef() + ")");
        }

        Iterator<MetaField> metaFieldIterator = docPartData.orderedMetaFieldIterator();
        for (KVValue<?> value : docPartRow) {
            MetaField metaField = metaFieldIterator.next();
            if (value != null) {
                Field field = field(metaField.getIdentifier());

                setValue(field, value);
            }
        }
    }

    public DocPartRow getDocPartRow() {
        throw new RuntimeException("Not implemented yet");
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

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

package com.torodb.poc.backend.tables.records;

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;

import org.jooq.Field;
import org.jooq.impl.TableRecordImpl;

import com.torodb.kvdocument.values.KVValue;
import com.torodb.poc.backend.mocks.PathDocument;
import com.torodb.poc.backend.tables.PathDocTable;

/**
 *
 */
public class PathDocTableRecord extends TableRecordImpl<PathDocTableRecord> {

    private static final long serialVersionUID = -556457916;

    private final PathDocTable table;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------
    /**
     * Create a detached RootRecord
     * <p>
     * @param table
     */
    public PathDocTableRecord(PathDocTable table) {
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

    public void setRid(Integer value) {
        set(1, value);
    }

    public Integer getRid() {
        return (Integer) getValue(1);
    }

    public void setPid(Integer value) {
        set(2, value);
    }

    public Integer getPid() {
        return (Integer) getValue(2);
    }

    public void setSeq(Integer value) {
        set(3, value);
    }

    @Nullable
    public Integer getSequence() {
        return (Integer) getValue(3);
    }

    public void setPathDoc(PathDocument pathdoc) throws IllegalArgumentException {

        if (!table.getPath().equals(pathdoc.getPath())) {
            throw new IllegalArgumentException("Path of table " + table + " is " + table.getPath() + " which is "
                    + "different than the type of the given subdocument (" + pathdoc.getPath() + ")");
        }

        for (Map.Entry<String, KVValue<?>> entry : pathdoc.getValues().entrySet()) {
            //@gortiz: I think we can not use types here!
            Field f = field(entry.getKey());
            KVValue<?> v = entry.getValue();

            setValue(f, v);
        }
    }

    public PathDocument getPathDoc() {
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

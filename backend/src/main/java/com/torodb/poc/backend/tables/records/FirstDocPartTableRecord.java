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

import com.torodb.poc.backend.tables.FirstDocPartTable;

/**
 *
 */
public class FirstDocPartTableRecord extends AbstractDocPartTableRecord<FirstDocPartTableRecord> {

    private static final long serialVersionUID = -2389673664785004453L;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------
    /**
     * Create a detached RootRecord
     * <p>
     * @param table
     */
    public FirstDocPartTableRecord(FirstDocPartTable table) {
        super(table);
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

    public void setSeq(Integer value) {
        set(2, value);
    }

    public Integer getSeq() {
        return (Integer) getValue(2);
    }
}

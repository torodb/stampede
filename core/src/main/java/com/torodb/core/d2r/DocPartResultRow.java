/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.d2r;

import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.kvdocument.values.KVValue;
import javax.annotation.Nullable;

/**
 *
 */
public interface DocPartResultRow {

    public int getDid();

    public int getRid();

    public int getPid();

    @Nullable
    public Integer getSeq();

    public KVValue<?> getUserValue(int fieldIndex, FieldType fieldType);

}

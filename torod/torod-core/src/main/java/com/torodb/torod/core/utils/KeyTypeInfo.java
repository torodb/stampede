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


package com.torodb.torod.core.utils;

import java.util.List;

/**
 *
 */
public class KeyTypeInfo {
    private final int id;
    private final int hash;
    private final List<String> keys;

    public KeyTypeInfo(int id, int hash, List<String> keys) {
        this.id = id;
        this.hash = hash;
        this.keys = keys;
    }

    public int getId() {
        return id;
    }

    public int getHash() {
        return hash;
    }

    public List<String> getKeys() {
        return keys;
    }
    
}

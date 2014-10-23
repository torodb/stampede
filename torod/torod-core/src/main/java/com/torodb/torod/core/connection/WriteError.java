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

package com.torodb.torod.core.connection;

import com.torodb.torod.core.utils.Mongofication;
import java.io.Serializable;
import javax.annotation.concurrent.Immutable;
import javax.json.Json;
import javax.json.JsonObject;

/**
 *
 */
@Immutable
@Mongofication
public class WriteError implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int index;
    private final int code;
    private final String msg;

    public WriteError(int index, int code, String msg) {
        this.index = index;
        this.code = code;
        this.msg = msg;
    }

    public int getIndex() {
        return index;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public JsonObject toJson() {
        return Json.createObjectBuilder()
                .add("index", index)
                .add("code", code)
                .add("msg", msg)
                .build();
    }
    
}

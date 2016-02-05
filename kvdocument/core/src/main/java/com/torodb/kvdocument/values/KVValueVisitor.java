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

package com.torodb.kvdocument.values;

/**
 *
 */
public interface KVValueVisitor<Result, Arg> {

    public Result visit(KVBoolean value, Arg arg);

    public Result visit(KVNull value, Arg arg);

    public Result visit(KVArray value, Arg arg);

    public Result visit(KVInteger value, Arg arg);

    public Result visit(KVLong value, Arg arg);

    public Result visit(KVDouble value, Arg arg);

    public Result visit(KVString value, Arg arg);

    public Result visit(KVDocument value, Arg arg);
    
    public Result visit(KVMongoObjectId value, Arg arg);
    
    public Result visit(KVInstant value, Arg arg);
    
    public Result visit(KVDate value, Arg arg);
    
    public Result visit(KVTime value, Arg arg);

    public Result visit(KVBinary value, Arg arg);

    public Result visit(KVMongoTimestamp value, Arg arg);

}

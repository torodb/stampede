/*
 * MongoWP - KVDocument: Core
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.kvdocument.values;

/**
 *
 */
public class KVValueAdaptor<Result, Arg> implements KVValueVisitor<Result, Arg>{

    public Result defaultCase(KVValue<?> value, Arg arg) {
        return null;
    }
    
    @Override
    public Result visit(KVBoolean value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVNull value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVArray value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVInteger value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVLong value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVDouble value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVString value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVDocument value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVMongoObjectId value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVInstant value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVDate value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVTime value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVBinary value, Arg arg) {
        return defaultCase(value, arg);
    }

    @Override
    public Result visit(KVMongoTimestamp value, Arg arg) {
        return defaultCase(value, arg);
    }
    
}

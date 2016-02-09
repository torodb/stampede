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

import com.torodb.kvdocument.values.KVDocument.DocEntry;

/**
 *
 * @param <Arg>    The auxiliary argument of the pre and post methods. If it is not needed, {@link java.lang.Void} can
 *                 be used
 */
public class KVValueDFW<Arg> implements KVValueVisitor<Void, Arg> {

    protected void preKVValue(KVValue<?> value, Arg arg) {
    }

    protected void postKVValue(KVValue<?> value, Arg arg) {
    }

    protected void preInt(KVInteger value, Arg arg) {
    }

    protected void postInt(KVInteger value, Arg arg) {
    }

    @Override
    public Void visit(KVInteger value, Arg arg) {
        preKVValue(value, arg);
        preInt(value, arg);

        postInt(value, arg);
        postKVValue(value, arg);

        return null;
    }

    protected void preLong(KVLong value, Arg arg) {
    }

    protected void postLong(KVLong value, Arg arg) {
    }

    @Override
    public Void visit(KVLong value, Arg arg) {
        preKVValue(value, arg);
        preLong(value, arg);

        postLong(value, arg);
        postKVValue(value, arg);

        return null;
    }

    protected void preString(KVString value, Arg arg) {
    }

    protected void postString(KVString value, Arg arg) {
    }

    @Override
    public Void visit(KVString value, Arg arg) {
        preKVValue(value, arg);
        preString(value, arg);

        postString(value, arg);
        postKVValue(value, arg);

        return null;
    }

    protected void preDouble(KVDouble value, Arg arg) {
    }

    protected void postDouble(KVDouble value, Arg arg) {
    }

    @Override
    public Void visit(KVDouble value, Arg arg) {
        preKVValue(value, arg);
        preDouble(value, arg);

        postDouble(value, arg);
        postKVValue(value, arg);

        return null;
    }

    protected void preDoc(KVDocument value, Arg arg) {
    }

    protected void postDoc(KVDocument value, Arg arg) {
    }

    @Override
    public Void visit(KVDocument value, Arg arg) {
        preKVValue(value, arg);
        preDoc(value, arg);

        for (DocEntry<?> entry : value) {
            entry.getValue().accept(this, arg);
        }

        postDoc(value, arg);
        postKVValue(value, arg);

        return null;
    }

    protected void preBoolean(KVBoolean value, Arg arg) {
    }

    protected void postBoolean(KVBoolean value, Arg arg) {
    }
    
    @Override
    public Void visit(KVBoolean value, Arg arg) {
        preKVValue(value, arg);
        preBoolean(value, arg);
        
        postBoolean(value, arg);
        postKVValue(value, arg);
        
        return null;
    }

    protected void preNull(KVNull value, Arg arg) {
    }

    protected void postNull(KVNull value, Arg arg) {
    }

    @Override
    public Void visit(KVNull value, Arg arg) {
        preKVValue(value, arg);
        preNull(value, arg);
        
        postNull(value, arg);
        postKVValue(value, arg);
        
        return null;
    }

    protected void preArray(KVArray value, Arg arg) {
    }

    protected void postArray(KVArray value, Arg arg) {
    }

    @Override
    public Void visit(KVArray value, Arg arg) {
        preKVValue(value, arg);
        preArray(value, arg);
        
        for (KVValue<?> element : value) {
            element.accept(this, arg);
        }
        
        postArray(value, arg);
        postKVValue(value, arg);
        
        return null;
    }

    protected void preMongoObjectId(KVMongoObjectId value, Arg arg) {
    }

    protected void postMongoObjectId(KVMongoObjectId value, Arg arg) {
    }

    @Override
    public Void visit(KVMongoObjectId value, Arg arg) {
        
        preKVValue(value, arg);
        preMongoObjectId(value, arg);
        
        postMongoObjectId(value, arg);
        postKVValue(value, arg);
        
        return null;
    }

    protected void preDateTime(KVInstant value, Arg arg) {
    }

    protected void postDateTime(KVInstant value, Arg arg) {
    }

    @Override
    public Void visit(KVInstant value, Arg arg) {
        
        preKVValue(value, arg);
        preDateTime(value, arg);
        
        postDateTime(value, arg);
        postKVValue(value, arg);
        
        return null;
    }

    protected void preDate(KVDate value, Arg arg) {
    }

    protected void postDate(KVDate value, Arg arg) {
    }

    @Override
    public Void visit(KVDate value, Arg arg) {
        
        preKVValue(value, arg);
        preDate(value, arg);
        
        postDate(value, arg);
        postKVValue(value, arg);
        
        return null;
    }

    protected void preTime(KVTime value, Arg arg) {
    }

    protected void postTime(KVTime value, Arg arg) {
    }

    @Override
    public Void visit(KVTime value, Arg arg) {
        
        preKVValue(value, arg);
        preTime(value, arg);
        
        postTime(value, arg);
        postKVValue(value, arg);
        
        return null;
    }

    protected void preBinary(KVBinary value, Arg arg) {
    }
    
    protected void postBinary(KVBinary value, Arg arg) {
    }
    
    @Override
    public Void visit(KVBinary value, Arg arg) {
        preKVValue(value, arg);
        preBinary(value, arg);
        
        postBinary(value, arg);
        postKVValue(value, arg);
        
        return null;
    }

    protected void preMongoTimestamp(KVMongoTimestamp value, Arg arg) {
    }

    protected void postMongoTimestamp(KVMongoTimestamp value, Arg arg) {
    }

    @Override
    public Void visit(KVMongoTimestamp value, Arg arg) {
        preKVValue(value, arg);
        preMongoTimestamp(value, arg);

        postMongoTimestamp(value, arg);
        postKVValue(value, arg);

        return null;
    }
    
}

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


package com.torodb.torod.core.subdocument.values;

/**
 *
 * @param <Arg>
 */
public class ScalarValueDFW<Arg> implements ScalarValueVisitor<Void, Arg>{
    
    protected void preDefaultValue(ScalarValue value, Arg arg) {
    }

    protected void postDefaultValue(ScalarValue value, Arg arg) {
    }

    protected void preInt(ScalarInteger value, Arg arg) {
    }

    protected void postInt(ScalarInteger value, Arg arg) {
    }

    @Override
    public Void visit(ScalarInteger value, Arg arg) {
        preDefaultValue(value, arg);
        preInt(value, arg);

        postInt(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preLong(ScalarLong value, Arg arg) {
    }

    protected void postLong(ScalarLong value, Arg arg) {
    }

    @Override
    public Void visit(ScalarLong value, Arg arg) {
        preDefaultValue(value, arg);
        preLong(value, arg);

        postLong(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preString(ScalarString value, Arg arg) {
    }

    protected void postString(ScalarString value, Arg arg) {
    }

    @Override
    public Void visit(ScalarString value, Arg arg) {
        preDefaultValue(value, arg);
        preString(value, arg);

        postString(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preDouble(ScalarDouble value, Arg arg) {
    }

    protected void postDouble(ScalarDouble value, Arg arg) {
    }

    @Override
    public Void visit(ScalarDouble value, Arg arg) {
        preDefaultValue(value, arg);
        preDouble(value, arg);

        postDouble(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preBooleanValue(ScalarBoolean value, Arg arg) {
    }

    protected void postBooleanValue(ScalarBoolean value, Arg arg) {
    }
    
    @Override
    public Void visit(ScalarBoolean value, Arg arg) {
        preDefaultValue(value, arg);
        preBooleanValue(value, arg);
        
        postBooleanValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void preNullValue(ScalarNull value, Arg arg) {
    }

    protected void postNullValue(ScalarNull value, Arg arg) {
    }

    @Override
    public Void visit(ScalarNull value, Arg arg) {
        preDefaultValue(value, arg);
        preNullValue(value, arg);
        
        postNullValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void preArrayValue(ScalarArray value, Arg arg) {
    }

    protected void postArrayValue(ScalarArray value, Arg arg) {
    }

    @Override
    public Void visit(ScalarArray value, Arg arg) {
        preDefaultValue(value, arg);
        preArrayValue(value, arg);
        
        for (ScalarValue<?> e : value) {
            e.accept(this, arg);
        }
        
        postArrayValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void preMongoObjectIdValue(ScalarMongoObjectId value, Arg arg) {
    }

    protected void postMongoObjectIdValue(ScalarMongoObjectId value, Arg arg) {
    }

    @Override
    public Void visit(ScalarMongoObjectId value, Arg arg) {
        preDefaultValue(value, arg);
        preMongoObjectIdValue(value, arg);

        postMongoObjectIdValue(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preMongoTimestampValue(ScalarMongoTimestamp value, Arg arg) {
    }

    protected void postMongoTimestampValue(ScalarMongoTimestamp value, Arg arg) {
    }

    @Override
    public Void visit(ScalarMongoTimestamp value, Arg arg) {
        preDefaultValue(value, arg);
        preMongoTimestampValue(value, arg);

        postMongoTimestampValue(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preInstantValue(ScalarInstant value, Arg arg) {
    }

    protected void postInstantValue(ScalarInstant value, Arg arg) {
    }

    @Override
    public Void visit(ScalarInstant value, Arg arg) {
        preDefaultValue(value, arg);
        preInstantValue(value, arg);

        postInstantValue(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preDateValue(ScalarDate value, Arg arg) {
    }

    protected void postDateValue(ScalarDate value, Arg arg) {
    }

    @Override
    public Void visit(ScalarDate value, Arg arg) {
        preDefaultValue(value, arg);
        preDateValue(value, arg);

        postDateValue(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preTimeValue(ScalarTime value, Arg arg) {
    }

    protected void postTimeValue(ScalarTime value, Arg arg) {
    }

    @Override
    public Void visit(ScalarTime value, Arg arg) {
        preDefaultValue(value, arg);
        preTimeValue(value, arg);

        postTimeValue(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preBinaryValue(ScalarBinary value, Arg arg) {
    }

    protected void postBinaryValue(ScalarBinary value, Arg arg) {
    }

    @Override
    public Void visit(ScalarBinary value, Arg arg) {
        preDefaultValue(value, arg);
        preBinaryValue(value, arg);

        postBinaryValue(value, arg);
        postDefaultValue(value, arg);

        return null;
    }
}

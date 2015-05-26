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
public interface DocValueVisitor<Result, Arg> {

    public Result visit(BooleanValue value, Arg arg);

    public Result visit(NullValue value, Arg arg);

    public Result visit(ArrayValue value, Arg arg);

    public Result visit(IntegerValue value, Arg arg);

    public Result visit(LongValue value, Arg arg);

    public Result visit(DoubleValue value, Arg arg);

    public Result visit(StringValue value, Arg arg);

    public Result visit(ObjectValue value, Arg arg);
    
    public Result visit(TwelveBytesValue value, Arg arg);
    
    public Result visit(DateTimeValue value, Arg arg);
    
    public Result visit(DateValue value, Arg arg);
    
    public Result visit(TimeValue value, Arg arg);

    public Result visit(PatternValue value, Arg arg);
    
}

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
package com.torodb.kvdocument.types;

/**
 *
 * @param <Result>
 * @param <Arg>
 */
public interface KVTypeVisitor<Result, Arg> {

    public Result visit(ArrayType type, Arg arg);

    public Result visit(BooleanType type, Arg arg);

    public Result visit(DoubleType type, Arg arg);

    public Result visit(IntegerType type, Arg arg);

    public Result visit(LongType type, Arg arg);

    public Result visit(NullType type, Arg arg);

    public Result visit(DocumentType type, Arg arg);

    public Result visit(StringType type, Arg arg);
    
    public Result visit(GenericType type, Arg arg);
    
    public Result visit(MongoObjectIdType type, Arg arg);
    
    public Result visit(InstantType type, Arg arg);
    
    public Result visit(DateType type, Arg arg);
    
    public Result visit(TimeType type, Arg arg);
    
    public Result visit(BinaryType type, Arg arg);

    public Result visit(NonExistentType type, Arg arg);

    public Result visit(MongoTimestampType type, Arg arg);

}

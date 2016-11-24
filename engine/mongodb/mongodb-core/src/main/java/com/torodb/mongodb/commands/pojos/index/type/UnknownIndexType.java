/*
 * ToroDB - ToroDB: MongoDB Core
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
package com.torodb.mongodb.commands.pojos.index.type;

import com.eightkdata.mongowp.bson.BsonValue;

public class UnknownIndexType extends AbstractIndexType {

    public UnknownIndexType(BsonValue<?> bsonValue) {
        super(bsonValue);
    }

    @Override
    public <Arg, Result> Result accept(IndexTypeVisitor<Arg, Result> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

}
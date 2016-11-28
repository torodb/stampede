/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.commands.impl.general;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.google.common.base.Splitter;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvValue;

public class AttrRefHelper {

  public static KvValue<?> calculateValueAndAttRef(BsonDocument doc,
      AttributeReference.Builder refBuilder) throws CommandFailed {
    if (doc.size() != 1) {
      throw new CommandFailed("find", "The given query is not supported right now");
    }
    Entry<?> entry = doc.getFirstEntry();

    for (String subKey : Splitter.on('.').split(entry.getKey())) {
      refBuilder.addObjectKey(subKey);
    }

    BsonValue<?> value = entry.getValue();
    if (value.isArray()) {
      throw new CommandFailed("find", "Filters with arrays are not supported right now");
    }
    if (value.isDocument()) {
      return calculateValueAndAttRef(value.asDocument(), refBuilder);
    } else {
      return MongoWpConverter.translate(value);
    }
  }

}

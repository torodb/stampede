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

package com.torodb.backend;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetaCollection;
import org.jooq.DSLContext;

import java.util.Collection;

import javax.annotation.Nonnull;

public interface WriteInterface {

  void insertDocPartData(@Nonnull DSLContext dsl, @Nonnull String schemaName,
      @Nonnull DocPartData docPartData) throws UserException;

  long deleteCollectionDocParts(@Nonnull DSLContext dsl, @Nonnull String schemaName,
      @Nonnull MetaCollection metaCollection, @Nonnull Cursor<Integer> didCursor);

  void deleteCollectionDocParts(@Nonnull DSLContext dsl, @Nonnull String schemaName,
      @Nonnull MetaCollection metaCollection, @Nonnull Collection<Integer> dids);

}

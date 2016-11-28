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

package com.torodb.core.d2r;

import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KvValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface R2DBackendTranslator<R, InternalFieldsT extends InternalFields> {

  /**
   * Position result to the next row if contains at least one more row.
   *
   * @param result
   * @return true if the result contained more rows
   * @throws RollbackException
   */
  @Nonnull
  boolean next(@Nonnull R result);

  /**
   * Read internal fields did, rid, pid and seq from result. Fields did, rid and pid must always
   * contain a non null value.
   *
   * @param metaDocPart
   * @param result
   * @return An instance of InternalFields with correctly filled did, rid, pid and seq.
   * @throws RollbackException
   */
  @Nonnull
  InternalFieldsT readInternalFields(@Nonnull MetaDocPart metaDocPart, @Nonnull R result);

  /**
   * Read the value of a metaField from result.
   *
   * @param type
   * @param result
   * @param internalFields
   * @param fieldIndex
   * @return The value readed from result or null if the value read is null.
   * @throws RollbackException
   */
  @Nullable
  KvValue<?> getValue(@Nonnull FieldType type, @Nonnull R result,
      @Nonnull InternalFieldsT internalFields,
      int fieldIndex);

}

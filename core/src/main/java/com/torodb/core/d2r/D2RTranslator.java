/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 *
 */
package com.torodb.core.d2r;

import com.torodb.kvdocument.values.KVDocument;
import javax.annotation.Nonnull;

/**
 *
 */
public interface D2RTranslator {

    /**
     * Translates from the document model to relational model the given argument.
     * <p>
     * The result is not returned on that function but appended to the accumulator associated with
     * this translator. In that way, several documents can be translated to the same
     * {@link CollectionData} to improve performance.
     * <p>
     * {@link #getCollectionDataAccumulator() } can be used to get the associated accumulator.
     *
     * @param doc the document that must be translated
     */
    public void translate(@Nonnull KVDocument doc);

    /**
     * A CollectionData that contains the translation of all documents that have been translated
     * with this translator.
     * @return
     */
    @Nonnull
    public CollectionData getCollectionDataAccumulator();

}

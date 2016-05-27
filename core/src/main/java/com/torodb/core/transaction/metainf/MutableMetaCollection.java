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

package com.torodb.core.transaction.metainf;

import com.torodb.core.TableRef;
import java.util.Collections;

/**
 *
 * @param <MDP>
 */
public interface MutableMetaCollection<MDP extends MutableMetaDocPart> extends MetaCollection<MetaDocPart> {

    public default MDP addDocPart(TableRef tableRef, String identifier) {
        return addDocPart(tableRef, identifier, Collections.emptyList());
    }

    public abstract MDP addDocPart(TableRef tableRef, String identifier, Iterable<ImmutableMetaField> fields);

    /**
     *
     * @param tableRef
     * @return
     * @throws IllegalArgumentException if {@link #getMetaDocPartByTableRef(com.torodb.core.TableRef)}
     *                                  returns null using the same reference
     */
    public abstract MDP asMutableDocPart(TableRef tableRef) throws IllegalArgumentException;

}

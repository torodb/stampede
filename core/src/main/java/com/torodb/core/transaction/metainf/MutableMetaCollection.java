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
import com.torodb.core.annotations.DoNotChange;
import java.util.stream.Stream;

/**
 *
 */
public interface MutableMetaCollection extends MetaCollection {

    @Override
    public MutableMetaDocPart getMetaDocPartByTableRef(TableRef tableRef);

    @Override
    public MutableMetaDocPart getMetaDocPartByIdentifier(String docPartId);

    @Override
    public Stream<? extends MutableMetaDocPart> streamContainedMetaDocParts();

    public MutableMetaDocPart addMetaDocPart(TableRef tableRef, String identifier) throws IllegalArgumentException;

    @DoNotChange
    public Iterable<? extends MutableMetaDocPart> getModifiedMetaDocParts();
    
    public abstract ImmutableMetaCollection immutableCopy();
}

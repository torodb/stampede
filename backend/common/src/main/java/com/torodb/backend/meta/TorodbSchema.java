/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General PublicSchema License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General PublicSchema License for more details.
 *
 *     You should have received a copy of the GNU Affero General PublicSchema License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */
package com.torodb.backend.meta;


import java.util.List;

import org.jooq.Table;
import org.jooq.impl.SchemaImpl;

public class TorodbSchema extends SchemaImpl {

	private static final long serialVersionUID = -1813122131;

    public static final String IDENTIFIER = "torodb";

    /**
     * The reference instance of <code>torodb</code>
     */
    public static final TorodbSchema TORODB = new TorodbSchema();

    /**
     * No further instances allowed
     */
	protected TorodbSchema() {
		super(IDENTIFIER);
	}

	@Override
	public final List<Table<?>> getTables() {
	    throw new UnsupportedOperationException();
	}
}

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

package com.torodb.config.util;

import com.fasterxml.jackson.module.jsonSchema.factories.JsonSchemaFactory;
import com.fasterxml.jackson.module.jsonSchema.types.AnySchema;
import com.fasterxml.jackson.module.jsonSchema.types.ArraySchema;
import com.fasterxml.jackson.module.jsonSchema.types.BooleanSchema;
import com.fasterxml.jackson.module.jsonSchema.types.IntegerSchema;
import com.fasterxml.jackson.module.jsonSchema.types.NullSchema;
import com.fasterxml.jackson.module.jsonSchema.types.NumberSchema;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema;
import com.fasterxml.jackson.module.jsonSchema.types.StringSchema;

public class CustomJsonSchemaFactory extends JsonSchemaFactory {
	public AnySchema anySchema() {
		return new AnySchema();
	}

	public ArraySchema arraySchema() {
		return new ArraySchema();
	}

	public BooleanSchema booleanSchema() {
		return new BooleanSchema();
	}

	public IntegerSchema integerSchema() {
		return new IntegerSchema();
	}

	public NullSchema nullSchema() {
		return new NullSchema();
	}

	public NumberSchema numberSchema() {
		return new NumberSchema();
	}

	public ObjectSchema objectSchema() {
		ObjectSchema objectSchema = new ObjectSchema();
		objectSchema.setAdditionalProperties(ObjectSchema.NoAdditionalProperties.instance);
		return objectSchema;
	}

	public StringSchema stringSchema() {
		return new StringSchema();
	}
}
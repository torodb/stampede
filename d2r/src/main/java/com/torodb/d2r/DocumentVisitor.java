/*
 * MongoWP - ToroDB-poc: D2R Implementation
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
package com.torodb.d2r;

import com.torodb.d2r.D2Relational.DocConsumer;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVBinary;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDate;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInstant;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVMongoObjectId;
import com.torodb.kvdocument.values.KVMongoTimestamp;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVString;
import com.torodb.kvdocument.values.KVTime;
import com.torodb.kvdocument.values.KVValueVisitor;

public class DocumentVisitor implements KVValueVisitor<Void, DocConsumer> {

	@Override
	public Void visit(KVBoolean value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVNull value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVArray value, DocConsumer arg) {
		arg.consume(value);
		return null;
	}

	@Override
	public Void visit(KVInteger value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVLong value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVDouble value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVString value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVDocument value, DocConsumer arg) {
		arg.consume(value);
		return null;
	}

	@Override
	public Void visit(KVMongoObjectId value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVInstant value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVDate value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVTime value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVBinary value, DocConsumer arg) {
		return null;
	}

	@Override
	public Void visit(KVMongoTimestamp value, DocConsumer arg) {
		return null;
	}

}

package com.torodb.backend.d2r;

import com.torodb.backend.d2r.D2Relational.DocConsumer;
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

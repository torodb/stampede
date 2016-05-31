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
 * along with kvdocument-core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.kvdocument.values;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.TestVisitor.MyArg;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class TestVisitor<Row> extends KVValueAdaptor<Void, MyArg<Row>> {

    private final Callback<Row> callback;

    public TestVisitor(Callback<Row> callback) {
        this.callback = callback;
    }

    public void start(KVDocument doc) {
        doc.accept(this, new MyArg<Row>());
    }

    public TableRef getTableRef(@Nullable TableRef parentTableRef, DocEntry<?> docEntry) {
    }

    public TableRef getTableRef(@Nullable TableRef parentTableRef, int seq) {
    }

    @Override
    public Void visit(KVDocument value, @Nullable MyArg<Row> arg) {
        Row myRow = callback.callback(arg.myTableRef, null, arg.parentRow, value);

        for (DocEntry<?> docEntry : value) {
            TableRef childTableRef = getTableRef(arg.myTableRef, docEntry);

            MyArg<Row> childArg = new MyArg<Row>(myRow, childTableRef);

            docEntry.getValue().accept(this, childArg);
        }
        return null;
    }

    @Override
    public Void visit(KVArray value, @Nullable MyArg<Row> arg) {
        callback.callback(arg.myTableRef, null, arg.parentRow, value);

        int seq = 0;
        for (KVValue<?> child : value) {
            TableRef childTableRef = getTableRef(arg.myTableRef, seq);
            seq++;

            MyArg<Row> childArg = new MyArg<Row>(arg.parentRow, childTableRef);

            value.accept(this, childArg);
        }
        return null;
    }

    public static class TableRef {

    }

    public static class MyArg<Row> {
        Row parentRow;
        TableRef parentTableRef;
        TableRef myTableRef;

        public MyArg() {
        }

        public MyArg(Row parentRow, TableRef parentTableRef) {
            this.parentRow = parentRow;
            this.parentTableRef = parentTableRef;
        }
    }

    public static interface Callback<Row> {
        @Nonnull
        public Row callback(@Nonnull TableRef tableRef, @Nullable Integer seq, @Nullable Row parent, KVDocument value);

        @Nonnull
        public void callback(@Nonnull TableRef tableRef, @Nullable Integer seq, @Nullable Row parent, KVArray value);
    }

    public static class CallbackImpl implements Callback<RowImpl> {
        Multimap<String, RowImpl> multimap;
        Function<String, Integer> ridProvider;

        @Override
        public RowImpl callback(String tableRef, Integer seq, RowImpl parent, KVDocument value) {
            int myRid = ridProvider.apply(tableRef);
            RowImpl myRow = new RowImpl(parent.did, parent.rid, myRid, seq);

            multimap.put(tableRef, myRow);

            for (DocEntry<?> entry : value.iterator()) {
                entry.getValue()
            }

            return myRow;
        }
    }

    public static class RowImpl implements Iterable<KVValue<?>> {
        int did;
        int pid;
        int rid;
        Integer seq;
        Map<String, KVValue<?>> values;

        public RowImpl(int did, int pid, int rid, Integer seq) {
            this.did = did;
            this.pid = pid;
            this.rid = rid;
            this.seq = seq;
        }

        @Override
        public Iterator<KVValue<?>> iterator() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}

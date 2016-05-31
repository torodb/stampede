package com.torodb.poc.backend;

import java.util.ArrayList;
import java.util.Iterator;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.kvdocument.values.KVValue;

public class DocPartRowImpl extends ArrayList<KVValue<?>> implements DocPartRow {
        private static final long serialVersionUID = 1L;
        
        private final DocPartData docPartData;
        private final int did;
        private final int rid;
        private final Integer pid;
        private final Integer seq;
        
        public DocPartRowImpl(DocPartData docPartData, int did) {
            this.docPartData = docPartData;
            this.did = this.rid = did;
            this.pid = null;
            this.seq = null;
        }
        
        public DocPartRowImpl(DocPartData docPartData, int did, int rid) {
            this.docPartData = docPartData;
            this.did = did;
            this.rid = rid;
            this.pid = null;
            this.seq = null;
        }
        
        public DocPartRowImpl(DocPartData docPartData, int did, int rid, int pid) {
            this.docPartData = docPartData;
            this.did = did;
            this.rid = rid;
            this.pid = pid;
            this.seq = null;
        }
        
        public DocPartRowImpl(DocPartData docPartData, int did, int rid, int pid, int index) {
            this.docPartData = docPartData;
            this.did = did;
            this.rid = rid;
            this.pid = pid;
            this.seq = index;
        }
        
        @Override
        public int getDid() {
            return did;
        }

        @Override
        public int getRid() {
            return rid;
        }

        @Override
        public Integer getPid() {
            return pid;
        }

        @Override
        public Integer getSeq() {
            return seq;
        }
        
        @Override
        public Iterator<KVValue<?>> iterator() {
            return new Iterator<KVValue<?>>() {
                private final int globalCount = docPartData.columnCount();
                private final int count = size();
                private int index = 0;
                
                @Override
                public boolean hasNext() {
                    return index < globalCount;
                }

                @Override
                public KVValue<?> next() {
                    if (index < count) {
                        return get(index++);
                    }
                    
                    index++;
                    
                    return null;
                }
            };
        }
        
        protected void appendColumnValue(String identifier, KVValue<?> value, int index) {
            final int size = size();
            if (index == size) {
                add(value);
            } else if (index < size) {
                set(index, value);
            } else {
                for (int offset = size; offset < index; offset++) {
                    add(null);
                }
                add(value);
            }
        }
    }

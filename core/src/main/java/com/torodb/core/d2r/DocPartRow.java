package com.torodb.core.d2r;

import com.torodb.kvdocument.values.KVValue;

public interface DocPartRow extends Iterable<KVValue<?>> {
    public DocPartData getDocPartData();
    public int getDid();
    public int getRid();
    public Integer getPid();
    public Integer getSeq();
}

package com.torodb.core.d2r;
public class InternalFields {
    public final Integer did, rid, pid, seq;

    public InternalFields(Integer did, Integer rid, Integer pid, Integer seq) {
        super();
        this.did = did;
        this.rid = rid;
        this.pid = pid;
        this.seq = seq;
    }

    public Integer getDid() {
        return did;
    }

    public Integer getRid() {
        return rid;
    }

    public Integer getPid() {
        return pid;
    }

    public Integer getSeq() {
        return seq;
    }
}

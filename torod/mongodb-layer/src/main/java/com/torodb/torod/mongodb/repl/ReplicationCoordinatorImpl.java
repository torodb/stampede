
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.WriteConcernEnforcementResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.MemberState;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.mongodb.WriteConcern;
import org.bson.types.ObjectId;

/**
 *
 */
public class ReplicationCoordinatorImpl implements ReplicationCoordinator {

    @Override
    public void startReplication() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public MemberState getMemberState() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long getSlaveDelaySecs() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public WriteConcernEnforcementResult awaitReplication(OpTime ts, WriteConcern wc) {
        //TODO: trivial implementation
        return new WriteConcernEnforcementResult(
                wc,
                null,
                0,
                null,
                false,
                null,
                ImmutableList.<HostAndPort>of()
        );
    }

    @Override
    public void stepDown(boolean force, long waitTime, long stepDownTime) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean canAcceptWrites(String databaseName) {
        //TODO: trivial implementation
        return true;
    }

    @Override
    public void setMyLastAppliedOptime(OpTime optime) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setMyHeartbeatMessage(String message) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ObjectId getOurElectionId() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ObjectId getRID() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void incrementRollbackId() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getRollbackId() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean buildsIndexes() {
        //TODO: trivial implementation
        return true;
    }

}

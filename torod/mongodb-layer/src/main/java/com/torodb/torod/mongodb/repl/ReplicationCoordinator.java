
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.WriteConcernEnforcementResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.MemberState;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.ReplicaSetConfig;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.mongodb.WriteConcern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.bson.types.ObjectId;

/**
 *
 */
@ThreadSafe
public interface ReplicationCoordinator {

    /**
     * Initializes the replication system, preparing it to accept the other
     * method calles
     */
    public void startReplication();

    /**
     * Stop replication.
     */
    public void shutdown();

    public MemberState getMemberState();

    /**
     *
     * @return the delay this node is configured to have.
     */
    public long getSlaveDelaySecs();

    /**
     * Stops the current user thread up to {@linkplain WriteConcern#wtimeout 
     * some milliseconds} or until the given optime is replicated to a set of
     * nodes that satisfies the given write concern, whichever comes first.
     * @param ts
     * @param wc
     * @return
     */
    public @Nonnull WriteConcernEnforcementResult awaitReplication(OpTime ts, WriteConcern wc);

    /**
     * Makes this node relinquish its primary condition for a given number of
     * milliseconds.
     * 
     * By default it will a given number of milliseconds until some secondary
     * node reach him, but if force is true, then it will be downgraded
     * immediately
     * 
     * @param force
     * @param waitTime
     * @param stepDownTime 
     */
    public void stepDown(boolean force, long waitTime, long stepDownTime);

    public boolean canAcceptWrites(String databaseName);

    /**
     * Updates the last applied optime associated with this node.
     *
     * @param optime
     */
    public void setMyLastAppliedOptime(OpTime optime);

    /**
     * Updates the message this node includes in his heartbeat responses.
     * @param message
     */
    public void setMyHeartbeatMessage(String message);

    /**
     * Returns an local-unique id that identifies when this node became
     * primary or null if this node is not primary.
     * @return 
     */
    @Nullable
    public ObjectId getOurElectionId();

    /**
     * Returns an id that identify this node on the replica set.
     *
     * This id should be unique in the replica set, but it is not guaranteed.
     * @return
     */
    public ObjectId getRID();

    /**
     * Returns the id that identifies this node on the current 
     * {@linkplain ReplicaSetConfig replica set configuration} .
     * @return 
     */
    public int getId();

    public void incrementRollbackId();

    public int getRollbackId();

    public boolean buildsIndexes();

}

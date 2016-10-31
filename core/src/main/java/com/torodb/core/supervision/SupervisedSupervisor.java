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
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.supervision;

import org.apache.logging.log4j.Logger;

/**
 *
 */
public abstract class SupervisedSupervisor implements Supervisor {

    private final Supervisor mySupervisor;

    public SupervisedSupervisor(Supervisor supervisor) {
        this.mySupervisor = supervisor;
    }

    protected SupervisorDecision escalate(Object supervised, Throwable error) {
        return mySupervisor.onError(
                this,
                new EscalatingException(mySupervisor, this, supervised, error)
        );
    }

    protected SupervisorDecision escalate(Object supervised, Throwable error, Logger logger) {
        logger.debug("Escalating error on {} to {}", supervised, mySupervisor);
        return escalate(supervised, error);
    }
}

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

/**
 *
 */
public class EscalatingException extends RuntimeException {

    private static final long serialVersionUID = 38581834681239L;

    private transient Supervisor parentSupervisor = null;
    private transient Supervisor escalatingSupervisor = null;
    private transient Object errorSource = null;

    public EscalatingException(Supervisor parentSupervisor,
            Supervisor escalatingSupervisor, Object errorSource,
            Throwable cause) {
        super(cause);
        this.parentSupervisor = parentSupervisor;
        this.escalatingSupervisor = escalatingSupervisor;
        this.errorSource = errorSource;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Supervisor getParentSupervisor() {
        return parentSupervisor;
    }

    public Supervisor getEscalatingSupervisor() {
        return escalatingSupervisor;
    }

    public Object getErrorSource() {
        return errorSource;
    }
}

/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.repl.oplogreplier.analyzed;

import com.eightkdata.mongowp.server.api.oplog.CollectionOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public enum AnalyzedOpType {

  NOOP(false, false, false) {
    @Override
    protected AnalyzedOpType andThenInsert() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenUpdateMod() {
      return UPDATE_MOD;
    }

    @Override
    protected AnalyzedOpType andThenUpdateSet() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenUpsertMod() {
      return UPSERT_MOD;
    }

    @Override
    protected AnalyzedOpType andThenUpsertSet() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenDelete() {
      return DELETE;
    }
  },
  DELETE_CREATE(false, false, true) {
    @Override
    protected AnalyzedOpType andThenInsert() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenUpdateMod() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenUpdateSet() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenUpsertMod() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenUpsertSet() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenDelete() {
      return DELETE;
    }
  },
  UPDATE_MOD(true, true, true) {
    @Override
    protected AnalyzedOpType andThenInsert() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenUpdateMod() {
      return UPDATE_MOD;
    }

    @Override
    protected AnalyzedOpType andThenUpdateSet() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenUpsertMod() {
      return UPDATE_MOD;
    }

    @Override
    protected AnalyzedOpType andThenUpsertSet() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenDelete() {
      return UPDATE_DELETE;
    }
  },
  UPDATE_SET(true, false, true) {
    @Override
    protected AnalyzedOpType andThenInsert() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenUpdateMod() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenUpdateSet() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenUpsertMod() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenUpsertSet() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenDelete() {
      return UPDATE_DELETE;
    }
  },
  UPSERT_MOD(false, true, true) {
    @Override
    protected AnalyzedOpType andThenInsert() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenUpdateMod() {
      return UPDATE_MOD;
    }

    @Override
    protected AnalyzedOpType andThenUpdateSet() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenUpsertMod() {
      return UPSERT_MOD;
    }

    @Override
    protected AnalyzedOpType andThenUpsertSet() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenDelete() {
      return DELETE;
    }
  },
  DELETE(false, false, true) {
    @Override
    protected AnalyzedOpType andThenInsert() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenUpdateMod() {
      return ERROR;
    }

    @Override
    protected AnalyzedOpType andThenUpdateSet() {
      return ERROR;
    }

    @Override
    protected AnalyzedOpType andThenUpsertMod() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenUpsertSet() {
      return DELETE_CREATE;
    }

    @Override
    protected AnalyzedOpType andThenDelete() {
      return DELETE;
    }
  },
  UPDATE_DELETE(true, false, true) {
    @Override
    protected AnalyzedOpType andThenInsert() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenUpdateMod() {
      return ERROR;
    }

    @Override
    protected AnalyzedOpType andThenUpdateSet() {
      return ERROR;
    }

    @Override
    protected AnalyzedOpType andThenUpsertMod() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenUpsertSet() {
      return UPDATE_SET;
    }

    @Override
    protected AnalyzedOpType andThenDelete() {
      return UPDATE_DELETE;
    }
  },
  ERROR(true, false, true) {
    @Override
    protected AnalyzedOpType andThenInsert() {
      return ERROR;
    }

    @Override
    protected AnalyzedOpType andThenUpdateMod() {
      return ERROR;
    }

    @Override
    protected AnalyzedOpType andThenUpdateSet() {
      return ERROR;
    }

    @Override
    protected AnalyzedOpType andThenUpsertMod() {
      return ERROR;
    }

    @Override
    protected AnalyzedOpType andThenUpsertSet() {
      return ERROR;
    }

    @Override
    protected AnalyzedOpType andThenDelete() {
      return ERROR;
    }
  };

  private final boolean requiresMatch;
  private final boolean requiresFetch;
  private final boolean deletes;

  private AnalyzedOpType(boolean requiresMatch, boolean requiresFetch, boolean deletes) {
    this.requiresMatch = requiresMatch;
    this.requiresFetch = requiresFetch;
    this.deletes = deletes;
  }

  public boolean requiresToFetchToroId() {
    return requiresMatch || requiresFetch || deletes;
  }

  public boolean requiresMatch() {
    return requiresMatch;
  }

  public boolean requiresFetch() {
    return requiresFetch;
  }

  public boolean deletes() {
    return deletes;
  }

  @SuppressFBWarnings(value = {"BC_UNCONFIRMED_CAST"},
      justification = "Cast is ligthly enforced by class implementation. We ignore this but maybe "
          + "visitor pattern should be used to prevent errors")
  public AnalyzedOpType andThen(CollectionOplogOperation colOp) {
    switch (colOp.getType()) {
      case DELETE:
        return andThenDelete();
      case INSERT:
        return andThenInsert();
      case UPDATE: {
        UpdateOplogOperation updateOp = (UpdateOplogOperation) colOp;
        if (updateOp.isUpsert()) {
          if (UpdateActionsTool.isSetModification(updateOp)) {
            return andThenUpsertSet();
          } else {
            return andThenUpsertMod();
          }
        } else {
          if (UpdateActionsTool.isSetModification(updateOp)) {
            return andThenUpdateSet();
          } else {
            return andThenUpdateMod();
          }
        }
      }
      default: {
        throw new AssertionError("Unexpected oplog operation type on a collection oplog op: "
            + colOp.getType());
      }
    }
  }

  protected abstract AnalyzedOpType andThenInsert();

  protected abstract AnalyzedOpType andThenUpdateMod();

  protected abstract AnalyzedOpType andThenUpdateSet();

  protected abstract AnalyzedOpType andThenUpsertMod();

  protected abstract AnalyzedOpType andThenUpsertSet();

  protected abstract AnalyzedOpType andThenDelete();
}

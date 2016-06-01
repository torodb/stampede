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

package com.torodb.core.transaction.metainf.utils;

import com.torodb.core.transaction.metainf.*;
import java.util.Objects;

/**
 * An utility class that checks if changes on a {@link MutableMetaSnapshot} are compatible with
 * a given {@link ImmutableMetaSnapshot}.
 *
 * This checker does only care about {@link MutableMetaSnapshot#getModifiedDatabases() databases
 * that have been modified} (and deeper changes on them). It does not check if non modified
 * databases are compatible with the immutable snapshot.
 */
public class DefaultMergeChecker {

    private DefaultMergeChecker() {}

    public static void checkMerge(ImmutableMetaSnapshot currentSnapshot, MutableMetaSnapshot newSnapshot)
            throws UnmergeableException {
        Iterable<? extends MutableMetaDatabase> changes = newSnapshot.getModifiedDatabases();
        for (MutableMetaDatabase change : changes) {
            checkMerge(currentSnapshot, newSnapshot, change);
        }
    }

    private static CheckCase checkCompatibility(Object nameObject, Object idObject) {
        if (Objects.equals(nameObject, idObject)) {
            return CheckCase.OK;
        }
        if (nameObject != null) {
            return CheckCase.DIFFERENT_NAME;
        }
        if (idObject != null) {
            return CheckCase.DIFFERENT_ID;
        }
        assert nameObject == null && idObject == null; //it should return on the first condition
        throw new AssertionError();
    }

    private static void checkMerge(ImmutableMetaSnapshot currentSnapshot, MutableMetaSnapshot newSnapshot, MutableMetaDatabase change) throws UnmergeableException {
        ImmutableMetaDatabase byName = currentSnapshot.getMetaDatabaseByIdentifier(change.getIdentifier());
        ImmutableMetaDatabase byId = currentSnapshot.getMetaDatabaseByName(change.getName());

        switch (checkCompatibility(byName, byId)) {
            case OK:
                break ;
            case DIFFERENT_NAME:
                assert byName != null;
                throw new UnmergeableException(currentSnapshot, newSnapshot,
                        "There is a previous database whose name is " + byName.getName()
                                + " that has a different id. The previous element id is "
                                + byName.getIdentifier() + " and the new one is " + change.getIdentifier());
            case DIFFERENT_ID:
                assert byId != null;
                throw new UnmergeableException(currentSnapshot, newSnapshot,
                        "There is a previous database whose id is " + byId.getIdentifier()
                                + " that has a different name. The previous element name is "
                                + byId.getName() + " and the new one is " + change.getName());
        }

        for (MutableMetaCollection modifiedCollection : change.getModifiedCollections()) {
            checkMerge(currentSnapshot, newSnapshot, byName, modifiedCollection);
        }
    }

    private static void checkMerge(ImmutableMetaSnapshot currentSnapshot, MutableMetaSnapshot newSnapshot,
            ImmutableMetaDatabase db, MutableMetaCollection changed) throws
            UnmergeableException{
        ImmutableMetaCollection byName = db.getMetaCollectionByName(changed.getName());
        ImmutableMetaCollection byId = db.getMetaCollectionByIdentifier(changed.getIdentifier());

        switch (checkCompatibility(byName, byId)) {
            case OK:
                break ;
            case DIFFERENT_NAME:
                assert byName != null;
                throw new UnmergeableException(currentSnapshot, newSnapshot,
                        "There is a previous collection on " + db + " whose name is "
                                + byName.getName() + " that has a different id. The previous "
                                + "element id is " + byName.getIdentifier() + " and the new one is "
                                + changed.getIdentifier());
            case DIFFERENT_ID:
                assert byId != null;
                throw new UnmergeableException(currentSnapshot, newSnapshot,
                        "There is a previous collection on " + db + " whose id is "
                                + byId.getIdentifier()+ " that has a different name. The previous "
                                + "element name is " + byId.getName() + " and the new one is "
                                + changed.getIdentifier());
        }

        for (MutableMetaDocPart modifiedDocPart : changed.getModifiedMetaDocParts()) {
            checkMerge(currentSnapshot, newSnapshot, db, byName, modifiedDocPart);
        }
    }

    private static void checkMerge(ImmutableMetaSnapshot currentSnapshot, MutableMetaSnapshot newSnapshot,
            ImmutableMetaDatabase db, ImmutableMetaCollection col,
            MutableMetaDocPart changed) throws UnmergeableException {
        ImmutableMetaDocPart byRef = col.getMetaDocPartByTableRef(changed.getTableRef());
        ImmutableMetaDocPart byId = col.getMetaDocPartByIdentifier(changed.getIdentifier());

        switch (checkCompatibility(byRef, byId)) {
            case OK:
                break ;
            case DIFFERENT_NAME:
                assert byRef != null;
                throw new UnmergeableException(currentSnapshot, newSnapshot,
                        "There is a previous doc part on " + db + "." + col + " whose ref is "
                                + byRef.getTableRef() + " that has a different id. The previous "
                                + "element id is " + byRef.getIdentifier() + " and the new one is "
                                + changed.getIdentifier());
            case DIFFERENT_ID:
                assert byId != null;
                throw new UnmergeableException(currentSnapshot, newSnapshot,
                        "There is a previous doc part on " + db + "." + col + " whose id is "
                                + byId.getIdentifier()+ " that has a different ref. The previous "
                                + "element ref is " + byId.getTableRef() + " and the new one is "
                                + changed.getTableRef());
        }

        for (ImmutableMetaField addedMetaField : changed.getAddedMetaFields()) {
            checkMerge(currentSnapshot, newSnapshot, db, col, byRef, addedMetaField);
        }
    }

    private static void checkMerge(ImmutableMetaSnapshot currentSnapshot, MutableMetaSnapshot newSnapshot,
            ImmutableMetaDatabase db, ImmutableMetaCollection col, ImmutableMetaDocPart docPart,
            ImmutableMetaField changed) throws UnmergeableException {
        ImmutableMetaField byNameAndType = docPart.getMetaFieldByNameAndType(changed.getName(), changed.getType());
        ImmutableMetaField byId = docPart.getMetaFieldByIdentifier(changed.getIdentifier());

        switch (checkCompatibility(byNameAndType, byId)) {
            case OK:
                return ;
            case DIFFERENT_NAME:
                assert byNameAndType != null;
                throw new UnmergeableException(currentSnapshot, newSnapshot,
                        "There is a previous field on " + db + "." + col + "." + docPart + " whose "
                                + "name is " + byNameAndType.getName() + " and type is "
                                + byNameAndType.getType() + " that has a different id. The previous "
                                + "element id is " + byNameAndType.getIdentifier() + " and the new "
                                + "one is " + changed.getIdentifier());
            case DIFFERENT_ID:
                assert byId != null;
                throw new UnmergeableException(currentSnapshot, newSnapshot,
                        "There is a previous field on " + db + "." + col + "." + docPart +" whose "
                                + "id is " + byId.getIdentifier()+ " that has a different name or "
                                + "type. The previous element name is " + byId.getName()+ " and its "
                                + "type is " + byId.getType() + ". The name of the one is "
                                + changed.getName() + " and its type is " + changed.getType());
        }
    }

    private static enum CheckCase {
        OK,
        DIFFERENT_ID,
        DIFFERENT_NAME
    }

}

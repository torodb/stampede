/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.core.language.utils;

import com.google.common.base.Preconditions;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.core.subdocument.structure.StructureElementVisitor;
import com.torodb.torod.core.utils.TriValuedResult;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class AttributeReferenceResolver {

    private static final BasicTypeResolver basicTypeResolver
            = new BasicTypeResolver();
    private static final StructureResolver structureResolver
            = new StructureResolver();
    private static final DocStructureResolver docStructureResolver
            = new DocStructureResolver();

    /**
     * Returns the type associated to the given attribute reference in the given
     * structure element.
     * <p>
     * Null is never returned, but {@linkplain TriValuedResult#UNDECIDABLE} can
     * be returned iff when the referenced attribute is not an array and it is
     * directly contained in an array.
     * <p>
     * @param attRef
     * @param elem
     * @return
     */
    @Nonnull
    public static TriValuedResult<? extends BasicType> resolveBasicType(
            AttributeReference attRef,
            StructureElement elem) {
        return elem.accept(basicTypeResolver, new AbstractResolver.Argument(
                           attRef));
    }

    /**
     * Returns the {@link StructureElement} ({@linkplain DocStructure document}
     * or {@linkplain ArrayStructure array}) that contains the given path or
     * {@link TriValuedResult#NULL} if the path is not contained in the
     * structure.
     * 
     * It never returns {@linkplain TriValuedResult#UNDECIDABLE}.
     * <p>
     * @param attRef
     * @param elem
     * @return
     */
    @Nonnull
    public static TriValuedResult<? extends StructureElement> resolveStructureElement(
            AttributeReference attRef,
            StructureElement elem) {
        return elem.accept(structureResolver, new AbstractResolver.Argument(
                           attRef));
    }

    /**
     * Returns the {@link DocStructure} that contains the given path or
     * {@link TriValuedResult#NULL} if the path is not contained in the
     * structure.
     * <p>
     * @param attRef
     * @param elem
     * @return The doc structure that contains the given path or
     *         {@link  TriValuedResult#NULL} if the structure does not contain
     *         the given path or if there is no document that contains the given
     *         path (for example, if the input structure only contains arrays),
     *         but never {@link TriValuedResult#UNDECIDABLE}
     */
    @Nullable
    public static LastDocStructureAndRelativeReference resolveDocStructureElement(
            AttributeReference attRef,
            StructureElement elem) {

        TriValuedResult<? extends LastDocStructureAndRelativeReference> value
                = elem.accept(docStructureResolver,
                              new AbstractResolver.Argument(attRef));
        assert !value.isUndecidable();

        if (value.isNull()) {
            return null;
        }
        return value.getValue();
    }

    private static class BasicTypeResolver extends AbstractResolver<BasicType> {

        @Override
        TriValuedResult<? extends BasicType> emptyCase(DocStructure structure,
                                                       AbstractResolver.Argument arg) {
            return TriValuedResult.NULL;
        }

        @Override
        TriValuedResult<? extends BasicType> emptyCase(ArrayStructure structure,
                                                       AbstractResolver.Argument arg) {
            return TriValuedResult.createValue(BasicType.ARRAY);
        }

        @Override
        TriValuedResult<BasicType> finalStep(DocStructure structure,
                                             AttributeReference.ObjectKey key,
                                             AbstractResolver.Argument arg) {
            SubDocAttribute attribute = structure.getType().getAttribute(key
                    .getKey());
            if (attribute == null) {
                return TriValuedResult.NULL;
            }
            return TriValuedResult.createValue(attribute.getType());
        }

        @Override
        TriValuedResult<BasicType> finalStep(ArrayStructure structure,
                                             AttributeReference.ArrayKey key,
                                             AbstractResolver.Argument arg) {
            /*
             * As arrays in toro do not have a inner (or element) type, we
             * cannot extract, in general, the type of their elements. But we
             * can recognize if the path points to an element that is an array
             * (and then return BasicType.ARRAY) or not (and then return
             * BasicType.GENERIC)
             */
            StructureElement subStructure = structure.get(key.getIndex());
            if (subStructure == null) {
                return TriValuedResult.UNDECIDABLE;
            }
            if (subStructure instanceof ArrayStructure) {
                return TriValuedResult.createValue(BasicType.ARRAY);
            }
            else { //subStructure is a subdocument an subdocuments do not have basic type
                return TriValuedResult.NULL;
            }
        }
    }

    private static class StructureResolver extends AbstractResolver<StructureElement> {

        @Override
        TriValuedResult<? extends StructureElement> emptyCase(
                DocStructure structure,
                AbstractResolver.Argument arg) {
            return TriValuedResult.createValue(structure);
        }

        @Override
        TriValuedResult<? extends StructureElement> emptyCase(
                ArrayStructure structure,
                AbstractResolver.Argument arg) {
            return TriValuedResult.createValue(structure);
        }

        @Override
        TriValuedResult<StructureElement> finalStep(DocStructure structure,
                                                    AttributeReference.ObjectKey key,
                                                    AbstractResolver.Argument arg) {
            StructureElement subStructure = structure.getElements().get(key
                    .getKey());
            if (subStructure == null) {
                return TriValuedResult.NULL;
            }
            return TriValuedResult.createValue(subStructure);
        }

        @Override
        TriValuedResult<StructureElement> finalStep(ArrayStructure structure,
                                                    AttributeReference.ArrayKey key,
                                                    AbstractResolver.Argument arg) {
            StructureElement subStructure = structure.get(key.getIndex());
            if (subStructure == null) {
                return TriValuedResult.NULL;
            }
            return TriValuedResult.createValue(subStructure);
        }
    }

    private static class DocStructureResolver extends AbstractResolver<LastDocStructureAndRelativeReference> {

        @Override
        TriValuedResult<? extends LastDocStructureAndRelativeReference> emptyCase(
                DocStructure structure,
                AbstractResolver.Argument arg) {
            return TriValuedResult.createValue(
                    new LastDocStructureAndRelativeReference(structure, arg
                                                             .getRelativeKeys()));
        }

        @Override
        TriValuedResult<? extends LastDocStructureAndRelativeReference> emptyCase(
                ArrayStructure structure,
                AbstractResolver.Argument arg) {
            return TriValuedResult.NULL;
        }

        @Override
        TriValuedResult<? extends LastDocStructureAndRelativeReference> finalStep(
                DocStructure structure,
                AttributeReference.ObjectKey key,
                AbstractResolver.Argument arg) {
            return TriValuedResult.createValue(
                    new LastDocStructureAndRelativeReference(structure, arg
                                                             .getRelativeKeys()));
        }

        @Override
        TriValuedResult<? extends LastDocStructureAndRelativeReference> finalStep(
                ArrayStructure structure,
                AttributeReference.ArrayKey key,
                AbstractResolver.Argument arg) {
            return TriValuedResult.createValue(
                    new LastDocStructureAndRelativeReference(arg
                            .getLastDocStructure(), arg
                                                             .getRelativeKeysFromLastDocStructure()));
        }

    }

    private static abstract class AbstractResolver<R> implements
            StructureElementVisitor<TriValuedResult<? extends R>, AbstractResolver.Argument> {

        @Nonnull
        abstract TriValuedResult<? extends R> emptyCase(DocStructure structure,
                                                        AbstractResolver.Argument arg);

        @Nonnull
        abstract TriValuedResult<? extends R> emptyCase(ArrayStructure structure,
                                                        AbstractResolver.Argument arg);

        @Nonnull
        abstract TriValuedResult<? extends R> finalStep(DocStructure structure,
                                                        AttributeReference.ObjectKey key,
                                                        AbstractResolver.Argument arg);

        @Nonnull
        abstract TriValuedResult<? extends R> finalStep(ArrayStructure structure,
                                                        AttributeReference.ArrayKey key,
                                                        AbstractResolver.Argument arg);

        @Override
        public TriValuedResult<? extends R> visit(DocStructure structure,
                                                  AbstractResolver.Argument arg) {
            if (arg.isEmpty()) {
                return emptyCase(structure, arg);
            }

            AttributeReference.Key actualKey = arg.getActualKey();
            if (!(actualKey instanceof AttributeReference.ObjectKey)) {
                return TriValuedResult.NULL;
            }

            AttributeReference.ObjectKey key
                    = (AttributeReference.ObjectKey) actualKey;

            TriValuedResult result;

            if (arg.thereAreMoreKeys()) {
                StructureElement subStructure = structure.getElements().get(key
                        .getKey());
                if (subStructure == null) {
                    return TriValuedResult.NULL;
                }
                //the orden between addDocStructure and increment depth is important!
                arg.addDocStructure(structure);
                arg.incrementDepth();

                result = subStructure.accept(this, arg);

                //the orden between addDocStructure and increment depth is important!
                arg.decrementDepth();
                arg.removeDocStructure();

            }
            else {
                result = finalStep(structure, key, arg);
            }

            return result;
        }

        @Override
        public TriValuedResult<? extends R> visit(ArrayStructure structure,
                                                  AbstractResolver.Argument arg) {
            if (arg.isEmpty()) {
                return emptyCase(structure, arg);
            }

            AttributeReference.Key actualKey = arg.getActualKey();
            if (!(actualKey instanceof AttributeReference.ArrayKey)) {
                return TriValuedResult.NULL;
            }

            AttributeReference.ArrayKey key
                    = (AttributeReference.ArrayKey) actualKey;

            TriValuedResult result;

            if (arg.thereAreMoreKeys()) {
                StructureElement subStructure = structure.get(key.getIndex());
                if (subStructure == null) {
                    return TriValuedResult.NULL;
                }

                arg.incrementDepth();

                result = subStructure.accept(this, arg);

                arg.decrementDepth();
            }
            else {
                result = finalStep(structure, key, arg);
            }

            return result;
        }

        private static class Argument {

            private final List<AttributeReference.Key> keys;
            /**
             * depth goes from 0 to keys.size() -1
             */
            private int depth;
            /**
             * A queue that stores the visited documents in depth order (the
             * last is the deepest)
             */
            private final Deque<DocStructure> docStructureStack;
            /**
             * A queue that stores the depth of the visited documents (the last
             * is the deepest and last visited)
             */
            private final Deque<Integer> docStructureDepthStack;

            public Argument(AttributeReference attRef) {
                this(attRef.getKeys());
            }

            public Argument(List<AttributeReference.Key> keys) {
                this.keys = keys;
                depth = 0;
                docStructureStack = new LinkedList<DocStructure>();
                docStructureDepthStack = new LinkedList<Integer>();
            }

            public void addDocStructure(DocStructure structure) {
                docStructureStack.addLast(structure);
                docStructureDepthStack.addLast(depth);
            }

            public void removeDocStructure() {
                docStructureStack.removeLast();
                docStructureDepthStack.removeLast();
            }

            public DocStructure getLastDocStructure() {
                return docStructureStack.getLast();
            }

            public int getLastDocStructureDepth() {
                return docStructureDepthStack.getLast();
            }

            public int getDepth() {
                return depth;
            }

            public void incrementDepth() {
                Preconditions.checkState(keys.size() > depth + 1, "Depth "
                                         + (depth + 1)
                                         + " would be higher than expected with keys = "
                                         + keys);
                depth++;
            }

            public void decrementDepth() {
                Preconditions.checkState(depth > 0);
                depth--;
            }

            public boolean isEmpty() {
                return keys.isEmpty();
            }

            public boolean thereAreMoreKeys() {
                return depth < keys.size() - 1;
            }

            @Nonnull
            public AttributeReference.Key getActualKey() {
                return keys.get(depth);
            }

            public List<AttributeReference.Key> getRelativeKeys() {
                return keys.subList(depth, keys.size());
            }

            private List<AttributeReference.Key> getRelativeKeysFromLastDocStructure() {
                return keys.subList(getLastDocStructureDepth() + 1, depth);
            }
        }
    }

    public static class LastDocStructureAndRelativeReference {

        private final DocStructure lastDocStructure;
        private final List<AttributeReference.Key> relativeReference;

        public LastDocStructureAndRelativeReference(
                DocStructure lastDocStructure,
                List<AttributeReference.Key> relativeReference) {
            this.lastDocStructure = lastDocStructure;
            this.relativeReference = relativeReference;
        }

        public DocStructure getLastDocStructure() {
            return lastDocStructure;
        }

        public List<AttributeReference.Key> getRelativeReference() {
            return relativeReference;
        }
    }
}

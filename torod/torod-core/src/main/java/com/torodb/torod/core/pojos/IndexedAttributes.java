
package com.torodb.torod.core.pojos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.torod.core.language.AttributeReference;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class IndexedAttributes {
    private final ImmutableList<AttributeReference> attributes;
    private final ImmutableMap<AttributeReference, Boolean> orderingInfo;

    private IndexedAttributes(
            ImmutableList<AttributeReference> attributes,
            ImmutableMap<AttributeReference, Boolean> attToIndex) {
        this.orderingInfo = attToIndex;
        this.attributes = attributes;
    }

    public List<AttributeReference> getIndexedAttributes() {
        return attributes;
    }

    public boolean ascendingOrdered(AttributeReference attRef) {
        Boolean ascendigOrdered = orderingInfo.get(attRef);
        if (ascendigOrdered == null) {
            throw new IllegalArgumentException("Attribute " + attRef +
                    " is not indexed by this index");
        }
        return ascendigOrdered;
    }

    public boolean contains(AttributeReference attRef) {
        return orderingInfo.containsKey(attRef);
    }

    public Iterable<Map.Entry<AttributeReference, Boolean>> entrySet() {
        return orderingInfo.entrySet();
    }

    public static class Builder {

        private final ImmutableList.Builder<AttributeReference> attributes;
        private final ImmutableMap.Builder<AttributeReference, Boolean> orderingInfo;

        public Builder() {
            attributes = ImmutableList.builder();
            orderingInfo = ImmutableMap.builder();
        }

        public Builder addAttribute(AttributeReference attRef, boolean ascendingOrder) {
            attributes.add(attRef);
            orderingInfo.put(attRef, ascendingOrder);
            return this;
        }

        public IndexedAttributes build() {
            return new IndexedAttributes(
                    attributes.build(),
                    orderingInfo.build()
            );
        }
    }

}

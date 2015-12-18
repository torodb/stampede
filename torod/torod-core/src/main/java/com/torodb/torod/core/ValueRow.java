
package com.torodb.torod.core;

import com.google.common.base.Function;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 *
 */
public interface ValueRow<V> {

    public Set<Map.Entry<String, V>> entrySet();

    public Set<String> keySet();

    public Set<V> valueSet();

    @Nonnull
    public V get(String key) throws IllegalArgumentException;

    public void consume(ForEachConsumer<V> consumer);

    public static interface ForEachConsumer<V> {
        public void consume(String key, V value);
    }

    public static class TranslatorValueRow<I,O> implements ValueRow<O> {
        private final Function<I, O> function;
        private final ValueRow<I> innerRow;

        public TranslatorValueRow(ValueRow<I> innerRow, Function<I, O> function) {
            this.function = function;
            this.innerRow = innerRow;
        }

        @Override
        public Set<Entry<String, O>> entrySet() {
            Set<Entry<String, I>> innerSet = innerRow.entrySet();
            Set<Entry<String, O>> result = new HashSet<>(innerSet.size());
            for (Entry<String, I> entry : innerSet) {
                result.add(new SimpleEntry<>(entry.getKey(), function.apply(entry.getValue())));
            }
            return result;
        }

        @Override
        public Set<String> keySet() {
            return innerRow.keySet();
        }

        @Override
        public Set<O> valueSet() {
            Set<I> innerSet = innerRow.valueSet();
            Set<O> result = new HashSet<>(innerSet.size());
            for (I innerValue : innerSet) {
                result.add(function.apply(innerValue));
            }
            return result;
        }

        @Override
        public O get(String key) throws IllegalArgumentException {
            return function.apply(innerRow.get(key));
        }

        @Override
        public void consume(ForEachConsumer<O> consumer) {
            innerRow.consume(new ConsumerTranslator(consumer));
        }

        public static <I2,O2> Function<ValueRow<I2>, ValueRow<O2>> getBuilderFunction(Function<I2, O2> function) {
            return new BuilderFunction<>(function);
        }

        private static class BuilderFunction<I2, O2> implements Function<ValueRow<I2>, ValueRow<O2>> {

            private final Function<I2, O2> function;

            private BuilderFunction(Function<I2, O2> function) {
                this.function = function;
            }

            @Override
            public ValueRow<O2> apply(ValueRow<I2> input) {
                return new TranslatorValueRow<>(input, function);
            }

        }

        private class ConsumerTranslator implements ForEachConsumer<I> {

            private final ForEachConsumer<O> outerConsummer;

            private ConsumerTranslator(ForEachConsumer<O> outerConsummer) {
                this.outerConsummer = outerConsummer;
            }

            @Override
            public void consume(String key, I value) {
                outerConsummer.consume(key, function.apply(value));
            }

        }
    }
}

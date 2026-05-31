/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.codegen.CodeGenUtils.newProjection;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Used to partial update a field which representing a nested table. The data type of nested table
 * field is {@code ARRAY<ROW>}.
 */
public class FieldNestedPartialUpdateAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final int nestedFields;
    private final Projection keyProjection;
    private final FieldGetter[] fieldGetters;

    @Nullable private final Projection sequenceProjection;
    @Nullable private final RecordComparator sequenceComparator;
    private final boolean hasSequenceField;

    public FieldNestedPartialUpdateAgg(String name, ArrayType dataType, List<String> nestedKey) {
        this(name, dataType, nestedKey, Collections.emptyList());
    }

    public FieldNestedPartialUpdateAgg(
            String name,
            ArrayType dataType,
            List<String> nestedKey,
            List<String> nestedSequenceField) {
        super(name, dataType);
        RowType nestedType = (RowType) dataType.getElementType();
        this.nestedFields = nestedType.getFieldCount();
        checkArgument(!nestedKey.isEmpty());
        this.keyProjection = newProjection(nestedType, nestedKey);
        this.fieldGetters = new FieldGetter[nestedFields];
        for (int i = 0; i < nestedFields; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(nestedType.getTypeAt(i), i);
        }

        // If nestedSequenceField is set, we need to compare sequence fields to determine
        // whether to update. Only update when the new sequence is greater than the old one.
        if (!nestedSequenceField.isEmpty()) {
            this.sequenceProjection = newProjection(nestedType, nestedSequenceField);
            this.hasSequenceField = true;

            List<DataType> seqTypes = new ArrayList<>();
            int[] sortFields = new int[nestedSequenceField.size()];
            for (int i = 0; i < nestedSequenceField.size(); i++) {
                String fieldName = nestedSequenceField.get(i);
                seqTypes.add(nestedType.getTypeAt(nestedType.getFieldIndex(fieldName)));
                sortFields[i] = i;
            }
            this.sequenceComparator = newRecordComparator(seqTypes, sortFields);
        } else {
            this.sequenceProjection = null;
            this.sequenceComparator = null;
            this.hasSequenceField = false;
        }
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        InternalArray acc = (InternalArray) accumulator;
        InternalArray input = (InternalArray) inputField;

        List<InternalRow> rows = new ArrayList<>(acc.size() + input.size());
        addNonNullRows(acc, rows);
        addNonNullRows(input, rows);

        if (keyProjection != null) {
            // Map<BinaryRow, GenericRow> map = new HashMap<>();
            // for (InternalRow row : rows) {
            //     BinaryRow key = keyProjection.apply(row).copy();
            //     GenericRow toUpdate = map.computeIfAbsent(key, k -> new
            // GenericRow(nestedFields));
            //     partialUpdate(toUpdate, row);
            // }
            //
            // rows = new ArrayList<>(map.values());

            Map<BinaryRow, SequenceRow> map = new HashMap<>();
            for (InternalRow row : rows) {
                BinaryRow key = keyProjection.apply(row).copy();
                SequenceRow existing = map.get(key);

                if (existing == null) {
                    GenericRow toUpdate = new GenericRow(nestedFields);
                    partialUpdate(toUpdate, row);
                    BinaryRow seq =
                            hasSequenceField && sequenceProjection != null
                                    ? sequenceProjection.apply(row).copy()
                                    : null;
                    map.put(key, new SequenceRow(toUpdate, seq));
                } else {
                    if (hasSequenceField) {
                        checkNotNull(sequenceComparator);
                        checkNotNull(sequenceProjection);

                        BinaryRow inputSeq = sequenceProjection.apply(row);
                        int cmp = sequenceComparator.compare(inputSeq, existing.sequence);

                        if (cmp > 0) {
                            // Case A: The new line version is higher, and the original partial overwrite update is triggered normally, with the version number being advanced
                            partialUpdate(existing.toUpdate, row);
                            existing.sequence = inputSeq.copy();
                        } else {
                            // Case B: The new row version is lower (out-of-order data), so the null gap in the existing data is filled in reverse with non-empty fields from the late row
                            mergeLateRow(existing.toUpdate, row);
                        }
                    } else {
                        // When no sequence is configured, the original pure incremental local override behavior is maintained
                        partialUpdate(existing.toUpdate, row);
                    }
                }
            }

            // Convert back to the list of rows required by the original logic
            List<InternalRow> mergedRows = new ArrayList<>(map.size());
            for (SequenceRow seqRow : map.values()) {
                mergedRows.add(seqRow.toUpdate);
            }
            rows = mergedRows;
        }

        return new GenericArray(rows.toArray());
    }

    private void mergeLateRow(GenericRow toUpdate, InternalRow lateInput) {
        for (int i = 0; i < fieldGetters.length; i++) {
            if (toUpdate.isNullAt(i)) {
                FieldGetter fieldGetter = fieldGetters[i];
                Object field = fieldGetter.getFieldOrNull(lateInput);
                if (field != null) {
                    toUpdate.setField(i, field);
                }
            }
        }
    }

    private static class SequenceRow {
        final GenericRow toUpdate;
        BinaryRow sequence;

        SequenceRow(GenericRow toUpdate, BinaryRow sequenceRow) {
            this.toUpdate = toUpdate;
            this.sequence = sequenceRow;
        }
    }

    private void addNonNullRows(InternalArray array, List<InternalRow> rows) {
        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                continue;
            }
            rows.add(array.getRow(i, nestedFields));
        }
    }

    private void partialUpdate(GenericRow toUpdate, InternalRow input) {
        for (int i = 0; i < fieldGetters.length; i++) {
            FieldGetter fieldGetter = fieldGetters[i];
            Object field = fieldGetter.getFieldOrNull(input);
            if (field != null) {
                toUpdate.setField(i, field);
            }
        }
    }
}

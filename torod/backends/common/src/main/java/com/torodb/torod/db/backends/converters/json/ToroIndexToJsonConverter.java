
package com.torodb.torod.db.backends.converters.json;

import com.google.common.collect.Sets;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.pojos.DefaultNamedToroIndex;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import javax.json.*;
import org.jooq.Converter;

/**
 *
 */
public class ToroIndexToJsonConverter implements Converter<String, NamedToroIndex> {
    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String collectionName;
    
    private static final String ATTS_KEY = "atts";
    private static final String UNIQUE_KEY = "unique";
    private static final String NAME_KEY = "key";
    private static final String DESCENDING = "desc";

    public ToroIndexToJsonConverter(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }
    
    @Override
    public NamedToroIndex from(String databaseObject) {
        JsonReader reader = Json.createReader(new StringReader(databaseObject));
        JsonObject object = reader.readObject();

        IndexedAttributes.Builder builder = new IndexedAttributes.Builder();
        JsonArray attsArray = object.getJsonArray(ATTS_KEY);
        Set<Integer> descendingAttPos;
        if (object.containsKey(DESCENDING)) {
            JsonArray descArray = object.getJsonArray(DESCENDING);
            descendingAttPos = Sets.newHashSetWithExpectedSize(descArray.size());
            for (int i = 0; i < descArray.size(); i++) {
                descendingAttPos.add(descArray.getInt(i));
            }
        }
        else {
            descendingAttPos = Collections.emptySet();
        }
        
        for (int i = 0; i < attsArray.size(); i++) {
            String att = attsArray.getString(i);
            AttributeReference attRef = parseAttRef(att);
            if (descendingAttPos.contains(i)) {
                builder.addAttribute(attRef, false);
            }
            else {
                builder.addAttribute(attRef, true);
            }
        }
        
        return new DefaultNamedToroIndex(
                object.getString(NAME_KEY), 
                builder.build(), 
                databaseName, 
                collectionName, 
                object.getBoolean(UNIQUE_KEY, false)
        );
    }

    @Override
    public String to(NamedToroIndex userObject) {
        JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
        objectBuilder.add(NAME_KEY, userObject.getName());
        if (userObject.isUnique()) {
            objectBuilder.add(UNIQUE_KEY, true);
        }
        
        JsonArrayBuilder attsBuilder = Json.createArrayBuilder();
        JsonArrayBuilder descBuilder = Json.createArrayBuilder();
        int attPosition = 0;
        boolean hasDescending = false;
        for (Map.Entry<AttributeReference, Boolean> entry: userObject.getAttributes().entrySet()) {
            attsBuilder.add(entry.getKey().toString());
            
            if (!entry.getValue()) {
                descBuilder.add(attPosition);
                hasDescending = true;
            }
            attPosition++;
        }
        objectBuilder.add(ATTS_KEY, attsBuilder);
        if (hasDescending) {
            objectBuilder.add(DESCENDING, descBuilder);
        }
        
        StringWriter stringWriter = new StringWriter(200);
        
        JsonWriter jsonWriter = Json.createWriter(stringWriter);
        jsonWriter.writeObject(objectBuilder.build());
        return stringWriter.toString();
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }

    @Override
    public Class<NamedToroIndex> toType() {
        return NamedToroIndex.class;
    }

    private AttributeReference parseAttRef(String key) {
        //TODO: check attributes with '\.' characters
        //TODO: Check attributes references with array keys
        StringTokenizer tk = new StringTokenizer(key, ".");
        AttributeReference.Builder attRefBuilder = new AttributeReference.Builder();
        while (tk.hasMoreTokens()) {
            attRefBuilder.addObjectKey(tk.nextToken());
        }
        return attRefBuilder.build();
    }
    
}

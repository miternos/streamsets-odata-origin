package com.miternos.streamset.stage.util;

import com.google.gson.*;
import org.apache.commons.codec.binary.Base64;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

public final class JsonUtil {

    public static final String NEWLINE_REPLACE_REGEX = "(?:\\n|\\r)";

    private JsonUtil() {};

    private static final Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
            .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
            .setFieldNamingStrategy(LowerCaseFieldNamingStrategy.LOWER_CASE_FIELD_NAMING_STRATEGY).setPrettyPrinting()
            .registerTypeAdapter(byte[].class, new ByteArraySerializer()).create();
    
    
    public static Gson getGson(){
        return gson;
    }
    
    /**
     * if object=null, method returns "null"
     * removes newlines as default
     * 
     * @param object
     * @return String
     */
    public static String toJsonString(Object object) {
        return toJsonString(object, true);
    }

    /**
     * if object=null, method returns "null" if removeNewLines=true replaces newlines with single space
     * 
     * @param object
     * @param removeNewLines
     * @return String
     */
    public static String toJsonString(Object object, boolean removeNewLines) {
        String str = gson.toJson(object);
        if (removeNewLines) {
            str = str.replaceAll(NEWLINE_REPLACE_REGEX, " ");
        }
        return str;
    }

    /**
     * converts json to String
     *
     * @param str
     * @return JsonObject
     */
    public static JsonObject convertToJson(String str) {
        JsonObject json = gson.fromJson(str, JsonObject.class);
        return json;
    }
    
    /**
     * converts string to JsonArray
     *
     * @param str
     * @return JsonArray
     */
    public static JsonArray convertToJsonArray(String str) {
        JsonArray json = gson.fromJson(str, JsonArray.class);
        return json;
    }
    
    

    private static class ByteArraySerializer implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {
        @Override
        public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
            String encodedString = new String(Base64.encodeBase64(src));
            return new JsonPrimitive(encodedString);
        }

        public byte[] deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            String jsonString = json.getAsString();
            byte[] byteArr = Base64.decodeBase64(jsonString);
            return byteArr;
        }
    }
    
    private static class LowerCaseFieldNamingStrategy implements FieldNamingStrategy {

        static final LowerCaseFieldNamingStrategy LOWER_CASE_FIELD_NAMING_STRATEGY = new LowerCaseFieldNamingStrategy();

        public String translateName(final Field fieldName) {
            return fieldName.getName().toLowerCase();
        }

    }

}

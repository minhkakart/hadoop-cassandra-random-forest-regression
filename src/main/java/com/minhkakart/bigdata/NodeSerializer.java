package com.minhkakart.bigdata;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class NodeSerializer implements JsonSerializer<DecisionTree.Node> {
    @Override
    public JsonElement serialize(DecisionTree.Node node, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject json = new JsonObject();
        if (node.left == null && node.right == null) {
            json.addProperty("value", node.value);
        } else {
            json.addProperty("feature", node.feature);
            json.addProperty("threshold", node.threshold);
            json.add("left", context.serialize(node.left));
            json.add("right", context.serialize(node.right));
        }
        return json;
    }
}

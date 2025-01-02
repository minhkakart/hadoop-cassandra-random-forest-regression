package com.minhkakart.bigdata.support;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.minhkakart.bigdata.algorithm.Node;

import java.lang.reflect.Type;

public class NodeSerializer implements JsonSerializer<Node> {
	@Override
	public JsonElement serialize(Node node, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject json = new JsonObject();
		if (node.getLeft() == null && node.getRight() == null) {
			json.addProperty("value", node.getValue());
		} else {
			json.addProperty("feature", node.getFeature());
			json.addProperty("threshold", node.getThreshold());
			json.add("left", context.serialize(node.getLeft()));
			json.add("right", context.serialize(node.getRight()));
		}
		return json;
	}
}

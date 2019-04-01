package org.conquernos.shover.arvo.decoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.ParsingDecoder;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;


public class MapDecoder extends ParsingDecoder implements Parser.ActionHandler {

	private Map<String, Object> in;

	private Stack<ReorderBuffer> reorderBuffers = new Stack<>();
	private ReorderBuffer currentReorderBuffer;

	private static class ReorderBuffer {
		Map<String, Map<String, Object>> savedFields = new HashMap<>();
		Map<String, Object> origParser = null;
	}

	public MapDecoder(Schema schema, Map<String, Object> in) throws IOException {
		this(getSymbol(schema), in);
	}

	private MapDecoder(Symbol root, Map<String, Object> in) throws IOException {
		super(root);
		configure(in);
	}

	private static Symbol getSymbol(Schema schema) {
		if (null == schema) throw new NullPointerException("Schema cannot be null!");

		return new JsonGrammarGenerator().generate(schema);
	}

	private MapDecoder configure(Map<String, Object> in) throws IOException {
		if (null == in) throw new NullPointerException("Map to read from cannot be null!");

		parser.reset();
		this.in = new TreeMap(in);

		return this;
	}

	private void advance(Symbol symbol) throws IOException {
		this.parser.processTrailingImplicitActions();
		parser.advance(symbol);
	}

	@Override
	public void readNull() throws IOException {
		if (in.containsKey(getKeyPathString())) throw error("null");
	}

	@Override
	public boolean readBoolean() throws IOException {
		advance(Symbol.BOOLEAN);
		String val = (String)in.get(getKeyPathString());
		if (Objects.equals(val, Boolean.toString(true)) || Objects.equals(val, Boolean.toString(false))) {
			return Objects.equals(val, Boolean.toString(true));
		} else {
			throw error("boolean");
		}
	}

	@Override
	public int readInt() throws IOException {
		advance(Symbol.INT);
		String val = (String)in.get(getKeyPathString());

		return Integer.parseInt(val);
	}

	@Override
	public long readLong() throws IOException {
		advance(Symbol.LONG);
		String val = (String)in.get(getKeyPathString());

		return Long.parseLong(val);
	}

	@Override
	public float readFloat() throws IOException {
		advance(Symbol.FLOAT);
		String val = (String)in.get(getKeyPathString());

		return Float.parseFloat(val);
	}

	@Override
	public double readDouble() throws IOException {
		advance(Symbol.DOUBLE);
		String val = (String)in.get(getKeyPathString());

		return Double.parseDouble(val);
	}

	@Override
	public Utf8 readString(Utf8 old) throws IOException {
		return new Utf8(readString());
	}

	@Override
	public String readString() throws IOException {
		advance(Symbol.STRING);
		if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
			advance(Symbol.MAP_KEY_MARKER);
			if (in.isEmpty()) throw error("map-key");

			String result = in.keySet().iterator().next();
			assert(result != null);

			// Strip the prefix consisting of the current keypath from this key
			if (!getKeyPathString().isEmpty()) {
				String keyPathPrefix = getKeyPathString() + "|";
				assert(result.startsWith(keyPathPrefix));
				result = result.substring(keyPathPrefix.length());
			}

			// Only get the next key, not the entire descending keypath
			if (result.indexOf('|') != -1) {
				result = result.substring(0, result.indexOf('|'));
			}

			pushKeyPathComponent(result);

			return result;
		} else {
			if (in.isEmpty()) throw error("string");

			return (String)in.get(getKeyPathString());
		}
	}

	@Override
	public void skipString() throws IOException {
		readString();
	}

	@Override
	public ByteBuffer readBytes(ByteBuffer old) throws IOException {
		return null;
	}

	@Override
	public void skipBytes() throws IOException {

	}

	@Override
	public void readFixed(byte[] bytes, int start, int length) throws IOException {

	}

	@Override
	public void skipFixed() throws IOException {
	}

	@Override
	public void skipFixed(int length) throws IOException {

	}

	@Override
	public int readEnum() throws IOException {
		return 0;
	}

	@Override
	public long readArrayStart() throws IOException {
		return 0;
	}

	@Override
	public long arrayNext() throws IOException {
		return 0;
	}

	@Override
	public long skipArray() throws IOException {
		return 0;
	}

	@Override
	public long readMapStart() throws IOException {
		advance(Symbol.MAP_START);

		return doMapNext(true);
	}

	@Override
	public long mapNext() throws IOException {
		advance(Symbol.ITEM_END);

		return doMapNext(false);
	}

	private long doMapNext(boolean fromReadMapStart) throws IOException {
		if (!fromReadMapStart) {
			in.remove(getKeyPathString());
			popKeyPathComponent();
		}

		long numLeft = 0;
		for (String key : in.keySet()) {
			if (key.startsWith(getKeyPathString())) numLeft++;
		}

		if (numLeft == 0 && !fromReadMapStart) {
			advance(Symbol.MAP_END);
		}

		return Math.min(1, numLeft);
	}

	@Override
	public long skipMap() throws IOException {
		throw new IOException("skipMap not implemented");
	}

	@Override
	public int readIndex() throws IOException {
		advance(Symbol.UNION);
		Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

		String label;
		if (in.isEmpty()) {
			label = "null";
		} else {
			label = stripKeyPathSuffix(stripKeyPathPrefix(in.keySet().iterator().next()));
			parser.pushSymbol(Symbol.UNION_END);
		}

		int n = a.findLabel(label);
		if (n < 0) throw new AvroTypeException("Unknown union branch " + label);
		parser.pushSymbol(a.getSymbol(n));
		pushKeyPathComponent(label);

		return n;
	}

	private AvroTypeException error(String type) {
		return new AvroTypeException(
			"Expected " + type + ". Got " + in.toString()
		);
	}

	@Override
	public Symbol doAction(Symbol input, Symbol top) throws IOException {
		if (top instanceof Symbol.FieldAdjustAction) {
			Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
			String name = fa.fname;
			pushKeyPathComponent(name);

			if (currentReorderBuffer != null) {
				Map<String, Object> savedSubMap = currentReorderBuffer.savedFields.get(name);
				if (savedSubMap != null) {
					currentReorderBuffer.savedFields.remove(name);
					currentReorderBuffer.origParser = in;
					this.in = new HashMap<>(savedSubMap);
					return null;
				}
			}

			if (in.size() > 0) {
				do {
					String nextKey = in.keySet().iterator().next();
					String fn = !nextKey.contains("|") ? nextKey : nextKey.substring(0, nextKey.indexOf("|"));
					if (name.equals(fn)) {
						return null;
					} else {
						if (currentReorderBuffer == null) {
							currentReorderBuffer = new ReorderBuffer();
						}
						Map<String, Object> subMap = new HashMap<>();
						Iterator it = in.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, Object> entry = (Map.Entry<String, Object>) it.next();
							String keyPrefix = fn + "|";
							if (entry.getKey().startsWith(keyPrefix) || entry.getKey().equals(fn)) {
								//String keyWithoutPrefix = entry.getKey().substring(keyPrefix.length());
								subMap.put(/*keyWithoutPrefix*/fn, entry.getValue());
								it.remove();
							}
						}
						currentReorderBuffer.savedFields.put(fn, subMap);
					}
				} while (in.size() > 0);

				throw new AvroTypeException("Expected field name not found: " + fa.fname);
			}
		} else if (top == Symbol.FIELD_END) {
			if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
				in = currentReorderBuffer.origParser;
				currentReorderBuffer.origParser = null;
			}
			popKeyPathComponent();
		} else if (top == Symbol.RECORD_START) {
			reorderBuffers.push(currentReorderBuffer);
			currentReorderBuffer = null;
		} else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
			if (in.size() == 0) {
				if (top == Symbol.RECORD_END) {
					if (currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
						throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
					}
					currentReorderBuffer = reorderBuffers.pop();
				}
			} else {
				throw error(top == Symbol.RECORD_END ? "record-end" : "union-end");
			}
		} else {
			throw new AvroTypeException("Unknown action symbol " + top);
		}

		return null;
	}

	private ArrayList<String> keyPath = new ArrayList<>();

	private void pushKeyPathComponent(String component) {
		keyPath.add(component);
	}

	private void popKeyPathComponent() {
		keyPath.remove(keyPath.size() - 1);
	}

	private String getKeyPathString() {
		return StringUtils.join(keyPath, '|');
	}

	// otherKeyPath = "a|b|c", partialKeyPathToRemove="a|b" -> "c"
	private String stripKeyPathPrefix(String otherKeyPath) {
		if (!otherKeyPath.contains("|")) {
			return otherKeyPath;
		} else {
			String keyPathPrefix = getKeyPathString() + "|";

			return otherKeyPath.substring(keyPathPrefix.length());
		}
	}

	// "a|b|c" -> "a"
	private String stripKeyPathSuffix(String keyPath) {
		return !keyPath.contains("|") ? keyPath : keyPath.substring(0, keyPath.indexOf("|"));
	}

}

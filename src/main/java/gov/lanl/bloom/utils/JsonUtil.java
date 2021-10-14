package gov.lanl.bloom.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import gov.lanl.bloom.Args;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.json.BloomFilterConverter;
import orestes.bloomfilter.memory.BloomFilterMemory;
import orestes.bloomfilter.redis.BloomFilterRedis;

public class JsonUtil {
	// int numberofelements=200000000;
	// float prob = 0.003f;
	// int numbh = 5;
	// HashMethod hm = HashMethod.Murmur3;

	public static void download_as_csv(BloomFilter bfr, String filename, String jdir, String name,String aname) {
		// Gson gson = new Gson();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();

		JsonElement json = BloomFilterConverter.toJson(bfr);
		JsonObject root = json.getAsJsonObject();
		root.addProperty("name", bfr.config().name());
		System.out.println("name" + bfr.config().name());
		root.addProperty("archive", aname);
		root.addProperty("HashMethod", bfr.config().hashMethod().name());
		System.out.println(bfr.config().hashMethod().name());
		String f = root.get("b").getAsString();
		int m = root.get("m").getAsInt();
		//int m = 200000000;
		int k = root.get("h").getAsInt();
		BufferedWriter writer1;
		try {
			writer1 = Files.newBufferedWriter(Paths.get(jdir + filename + ".csv"));

			CSVPrinter csvPrinter = new CSVPrinter(writer1,
					CSVFormat.DEFAULT.withHeader("name", "archive", "HashMethod", "m", "k", "BF"));

			csvPrinter.printRecord(bfr.config().name(), "haw", bfr.config().hashMethod().name(), m, k, f);
			csvPrinter.flush();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// root.addProperty("prob", bfr.config().falsePositiveProbability());

		// root.addProperty("expected", bfr.config().expectedElements());
		// root.addProperty("pop",bfr.getEstimatedPopulation());

		// String hashMethod = root.get("HashMethod").getAsString();
		/*
		 * try { Path pathToJson = Paths.get(jdir + filename + ".json"); //FileWriter
		 * ffile = new FileWriter(jdir + filename + ".json");
		 * //ffile.write(root.toString());
		 * 
		 * BufferedWriter writer =
		 * Files.newBufferedWriter(pathToJson,StandardCharsets.UTF_8,
		 * StandardOpenOption.CREATE, StandardOpenOption.WRITE); gson.toJson(root,
		 * writer); } catch (IOException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */
	}

	public void unload_fromredis(String jdir) {
		MultipleBloomLoader bl = new MultipleBloomLoader();
		HashMap<String, BloomFilter> filters = bl.getfilters();
		for (Map.Entry entry : filters.entrySet()) {
			String name = (String) entry.getKey();
			BloomFilter bf = (BloomFilter) entry.getValue();
			download_as_csv(bf, name, jdir, name,"haw");
		}

	}

	public BloomFilter loadtorediscvs(String pathFile) {
		CSVParser csvParser = null;

		FileReader reader;
		try {
			reader = new FileReader(pathFile);

			csvParser = new CSVParser(reader,
					CSVFormat.DEFAULT.withDelimiter(',').withQuote(null).withFirstRecordAsHeader());

			for (CSVRecord record : csvParser) {
				String fn = record.get(0).trim();
				String arh = record.get(1).trim();
				String h = record.get(2).trim();
				String mtext = record.get(3).trim();
				int m = Integer.parseInt(mtext);
				String ktext = record.get(4).trim();
				int k = Integer.parseInt(ktext);
				String b = record.get(5).trim();
				byte[] bits = Base64.getDecoder().decode(b);

				FilterBuilder builder = new FilterBuilder(m, k).hashFunction(HashMethod.Murmur3).redisBacked(true)
						.redisHost("localhost").name(fn);
				BloomFilterMemory<String> filter = new BloomFilterMemory<>(builder.complete());
				System.out.println("init filter");
				filter.setBitSet(BitSet.valueOf(bits));
				System.out.println("set bitset filter");
				// BitSet bs = filter.getBitSet();
				// System.out.println(bs.size());
				return filter;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	public void loadtoredisjson(String pathFile) {
		// Gson gson = new Gson();
		/*
		 * File initialFile = new File(pathFile ); InputStream in = null; try { in = new
		 * FileInputStream(initialFile); } catch (FileNotFoundException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); } JsonReader jr = new
		 * JsonReader(new InputStreamReader(in)); jr.setLenient(true); //like the
		 * expetion says to accept the malformed json JsonParser jp = new JsonParser();
		 * JsonElement root = jp.parse(jr);
		 */

		// BloomFilter f = from_Json(root,String.class);
		BloomFilter f = from_Json(pathFile, String.class);
		// BloomFilter otherBf = BloomFilterConverter.fromJson(json,);
		// BloomFilterConverter.
	}
	// public static <T> BloomFilter<T> from_Json(JsonElement source,Class<T> type)
	// {

	public static <T> BloomFilter<T> from_Json(String pathFile, Class<T> type) {
		File jsonFile = new File(pathFile).getAbsoluteFile();

		DocumentContext documentContext = null;
		try {
			documentContext = JsonPath.parse(jsonFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("" + documentContext.read("$.m"));
		System.out.println("" + documentContext.read("$.h"));
		System.out.println("" + documentContext.read("$.name"));
		int m = documentContext.read("$.m");
		int k = documentContext.read("$.h");
		String fn = documentContext.read("$.name");
		// JsonObject root = source.getAsJsonObject();
		// int m = root.get("m").getAsInt();
		// int k = root.get("h").getAsInt();
		// String fn = root.get("name").getAsString();
		System.out.println("loading" + fn);
		// String hashMethod = root.get("HashMethod").getAsString();
		String b = documentContext.read("$.b");
		// byte[] bits = Base64.getDecoder().decode(root.get("b").getAsString());
		byte[] bits = Base64.getDecoder().decode(b);

		FilterBuilder builder = new FilterBuilder(m, k).hashFunction(HashMethod.Murmur3).redisBacked(true)
				.redisHost("localhost").name(fn);
		BloomFilterMemory<T> filter = new BloomFilterMemory<>(builder.complete());
		System.out.println("init filter");

		// BloomFilterRedis fil = new BloomFilterRedis(builder.complete());

		filter.setBitSet(BitSet.valueOf(bits));
		System.out.println("set bitset filter");
		// BitSet bs = filter.getBitSet();
		// System.out.println(bs.size());
		return filter;
	}

	public static String toHexString(byte[] bytes) {
		char[] hexArray = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
		char[] hexChars = new char[bytes.length * 2];
		int v;
		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v / 16];
			hexChars[j * 2 + 1] = hexArray[v % 16];
		}
		return new String(hexChars);
	}

	public static String getMd5(String input) {
		try {

			// Static getInstance method is called with hashing MD5
			MessageDigest md = MessageDigest.getInstance("MD5");

			// digest() method is called to calculate message digest
			// of an input digest() return array of byte
			byte[] messageDigest = md.digest(input.getBytes());

			// Convert byte array into signum representation
			BigInteger no = new BigInteger(1, messageDigest);

			// Convert message digest into hex value
			String hashtext = no.toString(16);
			while (hashtext.length() < 32) {
				hashtext = "0" + hashtext;
			}
			return hashtext;
		}

		// For specifying wrong message digest algorithms
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) {

		Args arg = new Args(args);
		Map arguments = arg.asMap();
		String jdir = arg.get("dir", "test");
		String action = arg.get("action", "");
		JsonUtil ju = new JsonUtil();
		//this method saves 16 filters 
		if (action.contentEquals("unloadfromredis")) {
			ju.unload_fromredis(jdir);
		}
		if (action.contentEquals("loadfromjson")) {
			ju.loadtoredisjson(jdir);
		}
		if (action.contentEquals("loadfromcvs")) {
			ju.loadtorediscvs(jdir);
		}

	}

}

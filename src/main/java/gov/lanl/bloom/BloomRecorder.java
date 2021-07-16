package gov.lanl.bloom;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.BitSet;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.json.BloomFilterConverter;
import orestes.bloomfilter.memory.BloomFilterMemory;

public class BloomRecorder {
	int numberofelements=200000000;
	float prob = 0.003f;
    int numbh  = 5;
    HashMethod hm = HashMethod.Murmur3;
	static void download_as_json(BloomFilter bfr,String filename,String jdir,String name ) { 		  		
		 JsonElement json = BloomFilterConverter.toJson(bfr);
		 JsonObject root = json.getAsJsonObject();
		         root.addProperty("name",  bfr.config().name());
		         root.addProperty("archive", "haw");
		         root.addProperty("HashMethod", bfr.config().hashMethod().name());
		         root.addProperty("prob", bfr.config().falsePositiveProbability());
		         root.addProperty("expected", bfr.config().expectedElements());
		         root.addProperty("pop",bfr.getEstimatedPopulation());
		       //String hashMethod = root.get("HashMethod").getAsString();
		try {

			FileWriter ffile = new FileWriter(jdir + filename + ".json");
			ffile.write(root.toString());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static BitSet from_Json(JsonElement source) {
        JsonObject root = source.getAsJsonObject();
        int m = root.get("m").getAsInt();
        int k = root.get("h").getAsInt();
        //String hashMethod = root.get("HashMethod").getAsString();
        byte[] bits = Base64.getDecoder().decode(root.get("b").getAsString());

        FilterBuilder builder = new FilterBuilder(m, k).hashFunction(HashMethod.Murmur3);

        BloomFilterMemory filter = new BloomFilterMemory(builder.complete());
        BitSet bs = filter.getBitSet();
        System.out.println(bs.size());
        return bs;
    }
	
/*	public BloomFilter<String> init_mem_filter(String filtername) {
		
		 
		 int numberofelements = Integer.parseInt( properties.get("numberofelements"));
		 float prob = Float.parseFloat( properties.get("probability"));
		 int numbh  = Integer.parseInt( properties.get("numberofhashes"));
			BloomFilter<String> bfr = new FilterBuilder(numberofelements, prob)
					//BloomFilter<String> bfr = new FilterBuilder()
					.name(filtername) // use a distinct name
					.hashFunction(HashMethod.Murmur3)
					.hashes(numbh)
					//.addReadSlave(host, port +1) //add slave
	                //.addReadSlave(host, port +2)
					.buildBloomFilter();
			;
		
    return bfr;
	}
	*/
	public static String toHexString(byte[] bytes) {
	    char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
	    char[] hexChars = new char[bytes.length * 2];
	    int v;
	    for ( int j = 0; j < bytes.length; j++ ) {
	        v = bytes[j] & 0xFF;
	        hexChars[j*2] = hexArray[v/16];
	        hexChars[j*2 + 1] = hexArray[v%16];
	    }
	    return new String(hexChars);
	}
	
	public static String getMd5(String input)
    {
        try {
  
            // Static getInstance method is called with hashing MD5
            MessageDigest md = MessageDigest.getInstance("MD5");
  
            // digest() method is called to calculate message digest
            //  of an input digest() return array of byte
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
			
		// String md = org.apache.commons.codec.digest.DigestUtils.md5hex("whatever");
			
		    String passClear = "cleartext";
		    MessageDigest md5 = null;
			try {
				md5 = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} // you can change it to SHA1 if needed!
		    md5.update(passClear.getBytes(), 0, passClear.length());
		    System.out.printf("MD5: %s: %s ", passClear, new BigInteger(1, md5.digest()).toString(16));
				//5ab677ec767735cebd67407005786016
	}
	
}

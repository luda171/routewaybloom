package gov.lanl.bloom;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.JsonElement;
import com.opencsv.CSVReader;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.json.BloomFilterConverter;

/**
 * Hello world!
 *
 */
public class App {
	static String[] archives = new String[] { "blarchive", "webcite", "archiveit", "loc", "uknationalarchives", "gcwa",
			"archive.is", "hr", "pt", "es", "swa", "si", "ba", "cat", "aueb", "yorku", "ukparliament", "perma", "bsb",
			"nrs", "is", "nli", "proni", "banq" };
	static String host = "localhost";
	static int port = 6379;
	static HashMap filters = new HashMap();

	static String truthdatadir = "/data/var/logs/aggregator/";

	static public void init_filters() {
		int i;
		for (i = 0; i < archives.length; i++) {
			
			String filterName = archives[i];
			System.out.println("archive filter name"+filterName);
			
			BloomFilter<String> bfr = new FilterBuilder(21000000, 0.01)
					.name(filterName) // use a distinct name
					.hashFunction(HashMethod.Murmur3)
					.hashes(12).redisBacked(true)
					.redisHost(host) // localhost
					.redisPort(port) // Default is standard 6379
					.buildBloomFilter();
			filters.put(filterName, bfr);
		}

	}

	String stripURL(String url) {

		if (url.startsWith("http://")) {
			url = url.replaceFirst("http://", "");
		}

		if (url.startsWith("https://")) {
			url = url.replaceFirst("http://", "");
		}

		if (url.startsWith("http:/")) {
			url = url.replaceFirst("http:/", "");
		}

		if (url.startsWith("https:/")) {
			url = url.replaceFirst("https:/", "");
		}

		System.out.println("url:" + url);

		return url;

	}

	
	 public static boolean isNullOrEmpty(String str) {
	        if(str != null && !str.trim().isEmpty())
	            return false;
	        return true;
	    }
	 
	 
	 public  void  load_fromFile(String lfile) {
			try {
				//HashMap<String, int[]> intCounter = new HashMap<String, int[]>();
				 HashMap<String, AtomicInteger> atomicCounter = new HashMap<String, AtomicInteger>();
				CSVReader wr = new CSVReader(new FileReader(lfile), ' ');			
				String[] line;

				while ((line = wr.readNext()) != null) {
					String url = line[0].trim();
					String surl = stripURL(url);
					String archives = line[1].trim();
					System.out.println("log"+archives);
					 String[] names=null;
					   if (archives!=null)
						     if (archives.contains(":")) {
					            names = archives.split(":");
					           }
						      else {
							  names= new String[1];
							  names[0]=archives;
				         
					         int i;
					         if (names.length>0) {
					         for (i = 0; i < names.length; i++) {
						     String name = names[i].trim();
						     //System.out.println("_name: "+name);
						     if (!isNullOrEmpty(name) && !name.equals("ia")) {					
						     System.out.println("name: "+name);
						     
						 
						    
						       AtomicInteger value = atomicCounter.get(name);						     
								if (value != null) {
									value.incrementAndGet();
								} else {
									atomicCounter.put(name, new AtomicInteger(1));
								}
						     
						     BloomFilter<String> bfr = (BloomFilter<String>) filters.get(name);	
					           if (bfr!=null) {
						           bfr.add(surl);
					             }
						     }
					         }
					         }
					}
				}
				//atomicCounter.forEach((key, value) -> System.out.println(key + ":" + value));
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			 
	 }
	 
	 public static void main(String[] args) {
		//System.out.println("Hello World!");
		 
        App ap= new App();
		init_filters();
		ap.load_fromFile(truthdatadir + "data.log");
		ap.load_fromFile(truthdatadir + "truthdata.log.1");
		
		
		
		int j;
		for (j = 0; j < archives.length; j++) {
			String filterName = archives[j];
			System.out.println("filtername:"+filterName);
			BloomFilter<String> bfr = (BloomFilter<String>) filters.get(filterName);
			//System.out.println("size:"+bfr.getSize());
			JsonElement json = BloomFilterConverter.toJson(bfr);

			try {

				FileWriter ffile = new FileWriter("/data2/redisdata_6379/json/" + filterName + ".json");
				ffile.write(json.toString());

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
}

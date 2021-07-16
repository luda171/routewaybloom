package gov.lanl.bloom;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

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
public class BloomLoader {
	
	static String host = "localhost";
	static int port = 6379;
	static HashMap filters = new HashMap();
	static PropConfig properties;
	

	

	static public BloomFilter<String> init_filter(String filtername) {
		
		 
		 int numberofelements = Integer.parseInt( properties.all().get("numberofelements"));
		 float prob = Float.parseFloat( properties.all().get("probability"));
		 int numbh  = Integer.parseInt( properties.all().get("numberofhashes"));
			BloomFilter<String> bfr = new FilterBuilder(numberofelements, prob)
					.name(filtername) // use a distinct name
					.hashFunction(HashMethod.Murmur3)
					.hashes(numbh)
					.redisBacked(true)
					.redisHost(host) // localhost
					.redisPort(port) // Default is standard 6379
					.buildBloomFilter();
			;
		
       return bfr;
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
	 
	 
	 public  void  load_fromFile(String lfile,BloomFilter<String> bf) {
			try {
				
				long count=0;
				long totalcount =0;
				//HashMap<String, AtomicInteger> atomicCounter = new HashMap<String, AtomicInteger>();
				CSVReader wr = new CSVReader(new FileReader(lfile), ' ');			
				String[] line;
				DescriptiveStatistics stats = new DescriptiveStatistics();
				while ((line = wr.readNext()) != null) {
					
					String url = line[0].trim();
					if (url.contains("warc.gz")) continue;
					if (url.length()<2) continue;
					//String surl = stripURL(url);
					if (!bf.contains(url)) {
						bf.add(url);
						double d = url.length();
						stats.addValue(d);
						count = count+1;
						totalcount = totalcount+1;
						
					}
					else {
						totalcount = totalcount+1;
					}
					
				    
					            
				}
				System.out.println("totalcount:"+totalcount);
				System.out.println("count:"+count);
				System.out.println("url lenghs mean "+stats.getMean());
				System.out.println("population variance "+stats.getPopulationVariance());
				System.out.println("standart deviation "+stats.getStandardDeviation());
				System.out.println("population standart deviation "+ Math.sqrt(stats.getPopulationVariance()));
				System.out.println("max lengh:"+ stats.getMax());
				System.out.println("min lengh:"+ stats.getMin());
				System.out.println ("histogram");   
				double[] data = stats.getValues();
				int l = data.length;
				System.out.println("data length"+l);
			    final int BIN_COUNT = 20;
			    long[] histogram = new long[BIN_COUNT];
			    EmpiricalDistribution distribution = new EmpiricalDistribution(BIN_COUNT);
			    distribution.load(data);
			    int k = 0;
			    for(SummaryStatistics sumstats: distribution.getBinStats())
			    {
			    	long s = sumstats.getN();
			    	
			        histogram[k++] = sumstats.getN();
			        double m = sumstats.getMax();
			        System.out.println("max:"+m);
			        System.out.println("bin"+k);
			        System.out.println(s);
			    }
			    
				//atomicCounter.forEach((key, value) -> System.out.println(key + ":" + value));
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			 
	 }
	 
	 public static void main(String[] args) {
		//System.out.println("Hello World!");
		 
        BloomLoader ap = new BloomLoader();
           properties = PropConfig.getInstance();
        //?I will change tnat after initial test
        String cdxfile = properties.all().get("cdxfile");
        String filtername =  properties.all().get("filtername");
        
                
		BloomFilter<String> br = init_filter(filtername);
		long start = System.nanoTime();
		ap.load_fromFile(cdxfile,br);
		long diff = System.nanoTime() - start;
		double elapsedTimeInSecond = (double) diff / 1000000000;
		System.out.println(elapsedTimeInSecond + " seconds");
		
		
		
			
	}
}

package gov.lanl.bloom.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import gov.lanl.bloom.Args;
import gov.lanl.bloom.BloomUtility;
import gov.lanl.bloom.PropConfig;
import gov.lanl.bloom.web.resources.InitBloomFiltersServlet;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;

public class MultipleBloomLoader {
	int numberofelements=200000000;
	float prob = 0.01f;
    int numbh  = 5;
    
    char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    
    static String host = "localhost";
	static int port = 6379;

   HashMap filters = new HashMap();
	
	static HashMap filtercounts = new HashMap();
	public void load_filters(){
		
		 for (int i =0;i < hexArray.length;i++)
	        {
	          String fn= "haw" + hexArray[i];
	          System.out.println(fn);
	          BloomFilter<String> br = new FilterBuilder(numberofelements, prob)
	  				//BloomFilter<String> bfr = new FilterBuilder()
	  				.name(fn) // use a distinct name
	  				.hashFunction(HashMethod.Murmur3)
	  				.hashes(numbh)
	  				.redisBacked(true)
	  				.redisHost(host) // localhost
	  				.redisPort(port) // Default is standard 6379
	  				//.addReadSlave(host, port +1) //add slave
	               // .addReadSlave(host, port +2)
	  				.buildBloomFilter();
	          
	          filters.put(fn,br);
	          filtercounts.put(fn,0);
	        }
	}
	
	 void load_cdxfile(String Fname,String gzip,CSVWriter cwr){
		 
		
		 
		 CSVReader wr=null;
		try {
			if (gzip.equals("true")) {
			GZIPInputStream gzipin = new GZIPInputStream(new FileInputStream(Fname));
			BufferedReader br = new BufferedReader(new InputStreamReader(gzipin));
			wr = new CSVReader(br,' ');
			}
			else {
			wr = new CSVReader(new FileReader(Fname), ' ');
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
			String[] line;
			//DescriptiveStatistics stats = new DescriptiveStatistics();
			try {
				while ((line = wr.readNext()) != null) {
					
					String url = line[0].trim();
					if (url.contains("warc.gz")) continue;
					if (url.length()<2) continue;
					long start = System.nanoTime();
					
					 String ha = getMd5(url);
					 long end = System.nanoTime();
					    
				     String filtername ="haw"+ha.charAt(0);
				     //System.out.println(filtername);
				     BloomFilter bf=(BloomFilter) filters.get(filtername);
					
					if (!bf.contains(url)) {
						bf.add(url);
						Integer totalcount =(Integer) filtercounts.get(filtername);
						totalcount = totalcount+1;
						filtercounts.put(filtername,totalcount);
						
					}
					else {
						//totalcount = totalcount+1;
					}
					
				    
					            
				}
				Set keyn = filtercounts.keySet();
				Iterator it=keyn.iterator();
				while(it.hasNext()) {
					String f =(String) it.next();
					Integer c=(Integer) filtercounts.get(f);
					System.out.print(c);
					
					 BloomFilter br=(BloomFilter) filters.get(f);
					 Double pop = br.getEstimatedPopulation();
		      	       int sz = br.getSize();
		      	       
		      	       int elc = br.getExpectedElements();
		      	       double p = br.getEstimatedFalsePositiveProbability();
		      	       double nf = br.getHashes();
		      	     
		      	       System.out.println ("expected elements:"+elc);
		      	       System.out.println ("populated elements:"+pop);
		      	       System.out.println ("size:"+sz);
		      	       System.out.println ("estimated false positive probability:"+p);
		      	       System.out.println (" number of hashes :"+nf);
		      	       String nextLine[] = {f,String.valueOf(c), String.valueOf(p),f,String.valueOf(pop),String.valueOf(elc),String.valueOf(sz)}; 
		  			cwr.writeNext(nextLine);  
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally {
				
				try {
					wr.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
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
		 
		 Args arg = new Args(args);
		 Map arguments = arg.asMap();
		 BufferedReader reader = null;
		 String fname = arg.get("dirfilename", "test");
		 host= arg.get("redishost", "localhost");
		 String sport = arg.get("redisport","6379" );
		 port  =Integer.parseInt(sport);  
		 String gzip = arg.get("gzip", "false");
		 MultipleBloomLoader mb = new MultipleBloomLoader();
		 long start = System.nanoTime();
		 CSVWriter cwr = null;
    	 File resfile = new File("loadstats.csv");
    		try {
    			cwr = new CSVWriter(new FileWriter(resfile));
    		} catch (IOException e2) {
    			// TODO Auto-generated catch block
    			e2.printStackTrace();
    		}
		 
		 try {
	            //reading from file where dirs
				reader = new BufferedReader(new FileReader(fname));

				String line;

				line = reader.readLine();

				while (line != null) {
					System.out.println("directory:" + line);
					//String fileName = "warc";
					List<String> results = new ArrayList<String>();
					File[] files = new File(line).listFiles();
					for (File file : files) {
						if (file.isFile()) {
							 long start1 = System.nanoTime();
							//System.out.println("filename:"+line +file.getName());
							String abspath=file.getAbsolutePath();
							 mb.load_filters();
							 mb.load_cdxfile(abspath,gzip,cwr);
							 long diff1 = System.nanoTime() - start1;
								double elapsedTimeInSecond1 = (double) diff1 / 1000000000;
								System.out.println("file"+ abspath+"loadind time"+elapsedTimeInSecond1 + " seconds");
							//results.add(file.getName());
						}
						
					}
		 
		cwr.flush();
		
					line = reader.readLine();
		
		
				}//while
				long diff = System.nanoTime() - start;
				double elapsedTimeInSecond = (double) diff / 1000000000;
				System.out.println("all load"+elapsedTimeInSecond + " seconds");	
		 } catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 finally {
			 try {
					cwr.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		 }
		 
	 }
}

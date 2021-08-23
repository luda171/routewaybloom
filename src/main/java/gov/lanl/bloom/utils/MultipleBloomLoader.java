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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import gov.lanl.bloom.Args;
import gov.lanl.bloom.BloomUtility;
import gov.lanl.bloom.PropConfig;
import gov.lanl.bloom.web.resources.InitBloomFiltersServlet;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class MultipleBloomLoader {
	int numberofelements=200000000;
	float prob = 0.01f;
    int numbh  = 5;
    
    char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    
    static String host = "localhost";
	static int port = 6379;

    HashMap filters = new HashMap();
	HashMap hfiles = new HashMap();
	
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
	  				.redisConnections(1)
	  				//.addReadSlave(host, port +1) //add slave
	                //.addReadSlave(host, port +2)
	  				.redisHost(host) // localhost
	  				.redisPort(port) // Default is standard 6379
	  				//.addReadSlave(host, port +1) //add slave
	               // .addReadSlave(host, port +2)
	  				.buildBloomFilter();
	          
	          filters.put(fn,br);
	          filtercounts.put(fn,0);
	        }
	}
	
	
	public void reload_filter(String fn){
				 
	        {
	         
	          System.out.println(fn);
	          BloomFilter<String> br = new FilterBuilder(numberofelements, prob)
	  				//BloomFilter<String> bfr = new FilterBuilder()
	  				.name(fn) // use a distinct name
	  				.hashFunction(HashMethod.Murmur3)
	  				.hashes(numbh)
	  				.redisBacked(true)
	  				.redisConnections(1)
	  				//.addReadSlave(host, port +1) //add slave
	                //.addReadSlave(host, port +2)
	  				.redisHost(host) // localhost
	  				.redisPort(port) // Default is standard 6379
	  				//.addReadSlave(host, port +1) //add slave
	               // .addReadSlave(host, port +2)
	  				.buildBloomFilter();
	          
	          filters.put(fn,br);
	          filtercounts.put(fn,0);
	        }
	}
	
	public void init_tmp_files(String dir) {
		 for (int i =0;i < hexArray.length;i++)
	        {
	          String fn= "haw" + hexArray[i];
	          System.out.println(fn);
		      CSVWriter cwr = null;
   	          File resfile = new File(dir+fn+".csv");
   	       try {
    			cwr = new CSVWriter(new FileWriter(resfile));
    		} catch (IOException e2) {
    			// TODO Auto-generated catch block
    			e2.printStackTrace();
    		}
   	          hfiles.put(fn,cwr);
	        }
	}
	public void close_tmp_files() {
		 for (int i =0;i < hexArray.length;i++)
	        {
	          String fn= "haw" + hexArray[i];
	          System.out.println(fn);
	          CSVWriter cwr = (CSVWriter)hfiles.get(fn);
		      
  	          
  	       try {
   			cwr.close();
   		} catch (IOException e2) {
   			// TODO Auto-generated catch block
   			e2.printStackTrace();
   		}
  	         
	        }
	}
	 void tmp_cdxfile(String Fname){
		 //CSVReader wr=null;
		 try {
			 //InputStreamReader in = new InputStreamReader(new FileInputStream(Fname), StandardCharsets.UTF_8);
			FileReader reader = new FileReader(Fname);
			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter(' ')) ;
			for(CSVRecord record : csvParser) {
				String url = record.get(0).trim();
				if (url.contains("warc.gz")) continue;
				if (url.length()<2) continue;
				long start = System.nanoTime();
				
				 String ha = getMd5(url);
				 long end = System.nanoTime();
				    
			     String filename = "haw"+ha.charAt(0);
			     CSVWriter hf = (CSVWriter) hfiles.get(filename);
			     String nextLine[] = {url};
			     hf.writeNext(nextLine);  
			}
			
			/*		
				 wr = new CSVReader(new FileReader(Fname), ' ');
			
			String[] line;
			
				while ((line = wr.readNext()) != null) {
					
					String url = line[0].trim();
					if (url.contains("warc.gz")) continue;
					if (url.length()<2) continue;
					long start = System.nanoTime();
					
					 String ha = getMd5(url);
					 long end = System.nanoTime();
					    
				     String filename = "haw"+ha.charAt(0);
				     CSVWriter hf = (CSVWriter) hfiles.get(filename);
				     String nextLine[] = {url};
				     hf.writeNext(nextLine);  
				     
				}*/
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	 }
	 
	 void load_cdxfile(String Fname,String gzip,CSVWriter cwr){
		 
			
		 
		/* CSVReader wr=null;
		 * this library has bug -- have to replace it
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
				*/
		 CSVParser csvParser = null;
		        try {
				FileReader reader = new FileReader(Fname);
				csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter(' ')) ;
				
				for(CSVRecord record : csvParser) {
					String url = record.get(0).trim();
					//String url = line[0].trim();
					if (url.contains("warc.gz")) continue;
					if (url.length()<2) continue;
					long start = System.nanoTime();
					
					 String ha = getMd5(url);
					 long end = System.nanoTime();
					    
				     String filtername ="haw"+ha.charAt(0);
				     //System.out.println(filtername);
				     BloomFilter bf=(BloomFilter) filters.get(filtername);
					
					//if (!bf.contains(url)) {
						bf.add(url);
						Integer totalcount =(Integer) filtercounts.get(filtername);
						totalcount = totalcount+1;
						if (totalcount % 1000000 == 0) {
					        System.out.println(filtername+totalcount);
					        //reload_filter(filtername);
					    }
						//Integer totalcount1 =(Integer) filtercounts.get("haw0");
						//if (totalcount1 % 200000 == 0) {
					     //   System.out.println("reload filters");
					     //   load_filters(); 
					    //}
						filtercounts.put(filtername,totalcount);
						
					//}
					//else {
						//totalcount = totalcount+1;
					//}
					
				    
					            
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
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally {
				
				try {
					//wr.close();
					csvParser.close();
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
	
	 
	 public  void  load_fromFile(String lfile,BloomFilter<String> bf) {
			try {
				
				long count=0;
				long totalcount =0;
				//HashMap<String, AtomicInteger> atomicCounter = new HashMap<String, AtomicInteger>();
				CSVReader wr = new CSVReader(new FileReader(lfile), ' ');			
				String[] line;
				//DescriptiveStatistics stats = new DescriptiveStatistics();
				while ((line = wr.readNext()) != null) {
					
					String url = line[0].trim();
					if (url.contains("warc.gz")) continue;
					if (url.length()<2) continue;
					//String surl = stripURL(url);
					if (!bf.contains(url)) {
						bf.add(url);
						//double d = url.length();
						//stats.addValue(d);
						count = count+1;
						totalcount = totalcount+1;
						
					}
					else {
						totalcount = totalcount+1;
					}
					
				    
					            
				}
				System.out.println(lfile);
				System.out.println("totalcount:"+totalcount);
				System.out.println("count:"+count);
				//System.out.println("url lenghs mean "+stats.getMean());
				//System.out.println("population variance "+stats.getPopulationVariance());
				//System.out.println("standart deviation "+stats.getStandardDeviation());
				//System.out.println("population standart deviation "+ Math.sqrt(stats.getPopulationVariance()));
				//System.out.println("max lengh:"+ stats.getMax());
				//System.out.println("min lengh:"+ stats.getMin());
				//System.out.println ("histogram");   
				//double[] data = stats.getValues();
				//int l = data.length;
				//System.out.println("data length"+l);
			    //final int BIN_COUNT = 20;
			    //long[] histogram = new long[BIN_COUNT];
			    //EmpiricalDistribution distribution = new EmpiricalDistribution(BIN_COUNT);
			    //distribution.load(data);
			    //int k = 0;
			   // for(SummaryStatistics sumstats: distribution.getBinStats())
			   // {
			    //	long s = sumstats.getN();
			    	
			     //   histogram[k++] = sumstats.getN();
			     //   double m = sumstats.getMax();
			     //   System.out.println("max:"+m);
			     //   System.out.println("bin"+k);
			      //  System.out.println(s);
			   // }
			    
				//atomicCounter.forEach((key, value) -> System.out.println(key + ":" + value));
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			 
	 }	 
	 
	 public static void tmp_main (String[] args) {
		 Args arg = new Args(args);
		 Map arguments = arg.asMap();
		 String dir = arg.get("dir", "test");
		 String fname = arg.get("filename", "test");
		 host= arg.get("redishost", "localhost");
		 String sport = arg.get("redisport","6379" );
		 port  = Integer.parseInt(sport);  
		 String gzip = arg.get("gzip", "false");
		 MultipleBloomLoader mb = new MultipleBloomLoader();
		 mb.init_tmp_files(dir) ;
		 mb.tmp_cdxfile(dir+fname);
		 mb.close_tmp_files();
		 System.out.println("finish preprocess");
		 for (int i =0;i < mb.hexArray.length;i++)
	        {
	          String fn= "haw" + mb.hexArray[i];
	          System.out.println(fn);
	          BloomFilter<String> br = new FilterBuilder(mb.numberofelements, mb.prob)
	  				//BloomFilter<String> bfr = new FilterBuilder()
	  				.name(fn) // use a distinct name
	  				.hashFunction(HashMethod.Murmur3)
	  				.hashes(mb.numbh)
	  				.redisBacked(true)
	  				//.redisConnections(1)
	  				//.addReadSlave(host, port +1) //add slave
	                //.addReadSlave(host, port +2)
	  				.redisHost(host) // localhost
	  				.redisPort(port) // Default is standard 6379
	  				//.addReadSlave(host, port +1) //add slave
	               // .addReadSlave(host, port +2)
	  				.buildBloomFilter();
	          long start1 = System.nanoTime();
	          mb.load_fromFile(dir+fn+".csv", br);
	          long diff1 = System.nanoTime() - start1;
				double elapsedTimeInSecond1 = (double) diff1 / 1000000000;
				System.out.println("file"+ fn+"loadind time"+elapsedTimeInSecond1 + " seconds");
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
	        }
		 
	 }
	 public static void main(String [] args) {
		 //this loads just one file
		 Args arg = new Args(args);
		 Map arguments = arg.asMap();
		 String dir = arg.get("dir", "test");
		 String fname = arg.get("filename", "test");
		 host= arg.get("redishost", "localhost");
		 String sport = arg.get("redisport","6379" );
		 port  = Integer.parseInt(sport);  
		 String gzip = arg.get("gzip", "false");
		 MultipleBloomLoader mb = new MultipleBloomLoader();
		 mb.load_filters();
		 //long start = System.nanoTime();
		 CSVWriter cwr = null;
    	 File resfile = new File("loadstats"+fname+".csv");
    	 
 		try {
 			cwr = new CSVWriter(new FileWriter(resfile));
 		} catch (IOException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 		}
 		 	
				 long start1 = System.nanoTime();
					//System.out.println("filename:"+line +file.getName());
					
					 //mb.load_filters();
					 mb.load_cdxfile(dir+fname,gzip,cwr);
					 long diff1 = System.nanoTime() - start1;
						double elapsedTimeInSecond1 = (double) diff1 / 1000000000;
						System.out.println("file"+ fname+"loadind time"+elapsedTimeInSecond1 + " seconds");
			
						
						try {
							cwr.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
		 
	 }
	 public static void __main(String[] args) {
		 //this loading from list of directories
		 Args arg = new Args(args);
		 Map arguments = arg.asMap();
		 BufferedReader reader = null;
		 String fname = arg.get("dirfilename", "test");
		 host= arg.get("redishost", "localhost");
		 String sport = arg.get("redisport","6379" );
		 port  =Integer.parseInt(sport);  
		 String gzip = arg.get("gzip", "false");
		 MultipleBloomLoader mb = new MultipleBloomLoader();
		 mb.load_filters();
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
							 //mb.load_filters();
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

package gov.lanl.bloom;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.gson.JsonElement;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.json.BloomFilterConverter;



public class BloomUtility {
	static PropConfig configproperties;
	static String host = "localhost";
	static int port = 6379;
	Map <String,String>properties=new HashMap();
	
	public BloomFilter<String> init_filter(String filtername) {
		
		 
		 int numberofelements = Integer.parseInt( properties.get("numberofelements"));
		 float prob = Float.parseFloat( properties.get("probability"));
		 int numbh  = Integer.parseInt( properties.get("numberofhashes"));
			BloomFilter<String> bfr = new FilterBuilder(numberofelements, prob)
					//BloomFilter<String> bfr = new FilterBuilder()
					.name(filtername) // use a distinct name
					.hashFunction(HashMethod.Murmur3)
					.hashes(numbh)
					.redisHost(host) // localhost
					.redisPort(port) // Default is standard 6379
					//.addReadSlave(host, port +1) //add slave
	                //.addReadSlave(host, port +2)
					.buildBloomFilter();
			;
		
      return bfr;
	}

	 public BloomFilter<String> init_filter_custom(String filtername) {
    int numberofelements = Integer.parseInt( properties.get("numberofelements"));
    int bitstouse  = Integer.parseInt( properties.get("numberofbits"));	 
	BloomFilter<String> bf2 = new FilterBuilder()
            .expectedElements(numberofelements) //elements
            .size(bitstouse) //bits to use
            .hashFunction(HashMethod.Murmur3) //our hash
            .buildBloomFilter();
	return bf2;
	}
	
	public BloomFilter<String> init_mem_filter(String filtername) {
		
		 
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

	
	static void do_samle_query() {
		DescriptiveStatistics stats = new DescriptiveStatistics();
		
	}
	
	
	 void do_distinctby_length(String lfile) {
		    
			try {
		 CSVReader wr = new CSVReader(new FileReader(lfile), ' ');			
			String[] line;
			DescriptiveStatistics stats = new DescriptiveStatistics();
			
				while ((line = wr.readNext()) != null) {
					
					String url = line[0].trim();
					if (url.contains("warc.gz")) continue;
					if (url.length()<2) continue;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	 void do_query_check(Boolean neg) { 
		 String cdxfile = properties.get("cdxfile");
	CSVReader wr = null;
	String filtername =  properties.get("filtername");
	String fname =  properties.get("logfile");
	 boolean check = false;
	 BloomFilter<String> br = init_filter(filtername);
	
	
	
	 BloomFilter<String> dup = init_filter("dtest");
	 File resfile = new File(fname);
	 DescriptiveStatistics stats = new DescriptiveStatistics();
	 long tot=0;
	 long ncount = 0;
	 long pcount = 0;
	 long totlength = 0;
	 CSVWriter cwr = null;
		try {
			cwr = new CSVWriter(new FileWriter(resfile));
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	
	try {
		wr = new CSVReader(new FileReader( cdxfile ),' ');
		String[] line;
		while ((line = wr.readNext()) != null) {
			String url = line[0].trim();
			
			if (url.contains("warc.gz")) continue;
			if (dup.contains(url)) {continue;}
			else {
				totlength = totlength+url.length();
				dup.add(url);
			}
			//neg==true means urls are checked not existed
			if(neg) url = url+"<>%";
			tot=tot+1;
			long start = System.nanoTime();
			if ( br.contains(url)) {
				pcount = pcount+1;
			}
			else {
				ncount = ncount+1;	
			}
			long diff = System.nanoTime() - start;
			//double elapsedTimeInSecond = (double) diff / 1_000_000_000;
			double difference = (diff) / 1e6; //Milliseconds
			
			String nextLine[] = {String.valueOf(tot), url,String.valueOf(difference)}; 
			cwr.writeNext(nextLine);
			stats.addValue(difference);
		}
		System.out.print("positive" + pcount);
		System.out.print("negative" + ncount);
		System.out.print("total" + tot);
		
		cwr.close();
		System.out.println("mean in Milliseconds"+stats.getMean());
		System.out.println("population variance in Milliseconds"+stats.getPopulationVariance());
		System.out.println("standart deviation in Milliseconds"+stats.getStandardDeviation());
		System.out.println("population standart deviation in Milliseconds"+ Math.sqrt(stats.getPopulationVariance()));
		dup.remove();
		double avrl = totlength/tot;
		System.out.print("avr url length:" + avrl);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
	}
	void do_over_formhashes() {
		
		HashMethod[] methods = {HashMethod.MD5,HashMethod.SHA256,HashMethod.SHA384,
    			HashMethod.SHA512,HashMethod.Murmur3KirschMitzenmacher,HashMethod.Murmur3,HashMethod.RNG,HashMethod.CRC32};
    
		 String[] filternames = new String[] {"harvest-2020_05_opt5_MD5","harvest-2020_05_opt5_SHA256","harvest-2020_05_opt5_SHA384",
				 "harvest-2020_05_opt5_SHA512", "harvest-2020_05_opt5_Murmur3KirschMitzenmacher",
				 "harvest-2020_05_opt5_Murmur3","harvest-2020_05_opt5_RNG","harvest-2020_05_opt5_CRC32"};
		 
		 
	}
	void  do_over(){

		HashMethod[] methods = {HashMethod.MD5,HashMethod.SHA256,HashMethod.SHA384,
    			HashMethod.SHA512,HashMethod.Murmur3KirschMitzenmacher,HashMethod.Murmur3,HashMethod.RNG,HashMethod.CRC32};
    
		 String[] filternames = new String[] {"harvest-2020_05_opt5_MD5","harvest-2020_05_opt5_SHA256","harvest-2020_05_opt5_SHA384",
				 "harvest-2020_05_opt5_SHA512", "harvest-2020_05_opt5_Murmur3KirschMitzenmacher",
				 "harvest-2020_05_opt5_Murmur3","harvest-2020_05_opt5_RNG","harvest-2020_05_opt5_CRC32"};
		 //String[] filternames = new String[] {"harvest-2020_3_05", "harvest-2020_054","harvest-2020_0545","harvest-2020_056","harvest-2020_0569","harvest-2020_056912","harvest-2020_05691215","harvest-2020_0569121518","harvest-2020_056912151821"};
		 CSVWriter cwr = null;
		 String fname =  properties.get("logfile");
		 File resfile = new File(fname);
			try {
				cwr = new CSVWriter(new FileWriter(resfile));
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
		String nextLine[] = {"filtername","neg","positivecount", "negcount","maxl","number_hs","avrtime_ms","st_dev"};
		cwr.writeNext(nextLine);
			
		 for (int i =0;i < filternames.length;i++)
	        {
	          String fn=  filternames[i];
	          HashMethod m = methods[i];
	          make_query_check(true,fn,0,cwr,m);
	          make_query_check(false,fn,0,cwr,m);
	          make_query_check(true,fn,200,cwr,m);
	          make_query_check(false,fn,200,cwr,m);
	        }
		 try {
			cwr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
	//temp function
	 void make_query_check(Boolean neg,String filtername,int max_l,CSVWriter cwr,HashMethod m) {
		 int maxl = 2083;
		 int step = 2083/20;
		 
         
	
		 String cdxfile = properties.get("cdxfile");
	    CSVReader wr = null;
	//String filtername =  properties.get("filtername");
	String fname =  properties.get("logfile");
	 boolean check = false;
	 //BloomFilter<String> br = init_filter(filtername);
	 int numberofelements = Integer.parseInt( properties.get("numberofelements"));
	 float prob = Float.parseFloat( properties.get("probability"));
	
	 BloomFilter<String> br = new FilterBuilder(numberofelements, prob)
				//BloomFilter<String> bfr = new FilterBuilder()
				.name(filtername) // use a distinct name
				.hashFunction(m)
				.hashes(5)
				.redisHost(host) // localhost
				.redisPort(port) // Default is standard 6379
				//.addReadSlave(host, port +1) //add slave
             //.addReadSlave(host, port +2)
				.buildBloomFilter();
	 
	 
	 
	 int hs = br.getHashes();
	 BloomFilter<String> dup = init_filter("dtest");
	 File resfile = new File(fname);
	 DescriptiveStatistics stats = new DescriptiveStatistics();
	 long tot=0;
	 long ncount = 0;
	 long pcount = 0;
	 //long totlength = 0;
	/* CSVWriter cwr = null;
		try {
			cwr = new CSVWriter(new FileWriter(resfile));
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}*/
	
	try {
		wr = new CSVReader(new FileReader( cdxfile ),' ');
		String[] line;
		while ((line = wr.readNext()) != null) {
			String url = line[0].trim();
			//dedup
			if (url.contains("warc.gz")) continue;
			//filtering by url length
			if (max_l>0) {
			 if(url.length()>max_l) continue;
			}
			if (dup.contains(url)) {continue;}
			else {
				//totlength = totlength+url.length();
				dup.add(url);
			}
			//
			
			
			//neg==true means urls are checked not existed
			if(neg) url = url+"<>%";
			tot=tot+1; 
			
			long start = System.nanoTime();
			if ( br.contains(url)) {
				pcount = pcount+1;
			}
			else {
				ncount = ncount+1;	
			}
			
			
			long diff = System.nanoTime() - start;
			//double elapsedTimeInSecond = (double) diff / 1_000_000_000;
			double difference = (diff) / 1e6; //Milliseconds
			
			//String nextLine[] = {String.valueOf(tot), url,String.valueOf(difference)}; 
			//cwr.writeNext(nextLine);
			stats.addValue(difference);
			if (tot==1000000) break;
		}
		System.out.print("positive" + pcount);
		System.out.print("negative" + ncount);
		System.out.print("total" + tot);
		
		//cwr.close();
		System.out.println("mean in Milliseconds"+stats.getMean());
		System.out.println("population variance in Milliseconds"+stats.getPopulationVariance());
		System.out.println("standart deviation in Milliseconds"+stats.getStandardDeviation());
		System.out.println("population standart deviation in Milliseconds"+ Math.sqrt(stats.getPopulationVariance()));
		
		String nextLine[] = {filtername,m.name(),String.valueOf(neg),String.valueOf(pcount), String.valueOf(ncount),String.valueOf(max_l),String.valueOf(hs),String.valueOf(stats.getMean()),String.valueOf(stats.getStandardDeviation())}; 
		cwr.writeNext(nextLine);
		
		dup.remove();
		//double avrl = totlength/tot;
		//System.out.print("avr url length:" + avrl);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
	}
	
	 
	static void download_as_json(BloomFilter bfr,String filename,String jdir ) { 
		 //BloomFilter<String> bfr = init_filter(filtername);
		 JsonElement json = BloomFilterConverter.toJson(bfr);

		try {

			FileWriter ffile = new FileWriter(jdir + filename + ".json");
			ffile.write(json.toString());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	void load_different_prob() {
		 BloomLoader ap = new BloomLoader();
		 String cdxfile = properties.get("cdxfile");
         String filtername =  properties.get("filtername");
         String fname =  properties.get("logfile");
         int numberofelements = Integer.parseInt( (String)properties.get("numberofelements"));
         long totlength = 0;
    	 CSVWriter cwr = null;
    	 File resfile = new File(fname);
    		try {
    			cwr = new CSVWriter(new FileWriter(resfile));
    		} catch (IOException e2) {
    			// TODO Auto-generated catch block
    			e2.printStackTrace();
    		}
    	 float [] prob = {0.001f,0.003f,0.005f,0.01f};	
    	 int i;
    	 for (i = 0; i < prob.length; i++) {
    		 float cprob=prob[i];
    		 String s=String. valueOf(cprob);
    		 String filtername1 = filtername+s;
    		 BloomFilter<String> br = new FilterBuilder(numberofelements, cprob)
  					//BloomFilter<String> bfr = new FilterBuilder()
  					.name(filtername1) // use a distinct name
  					.hashFunction(HashMethod.Murmur3)
  					.hashes(5)
  					.redisHost(host) // localhost
  					.redisPort(port) // Default is standard 6379
  					//.addReadSlave(host, port +1) //add slave
  	                //.addReadSlave(host, port +2)
  					.buildBloomFilter();
    		 long start = System.nanoTime();
  			ap.load_fromFile(cdxfile,br);
  			long diff = System.nanoTime() - start;
  			double elapsedTimeInSecond = (double) diff / 1000000000;
  			System.out.println(elapsedTimeInSecond + " seconds");
           
  			 Double pop = br.getEstimatedPopulation();
    	       int sz = br.getSize();
    	       
    	       int elc = br.getExpectedElements();
    	       double p = br.getEstimatedFalsePositiveProbability();
    	       double nf = br.getHashes();
    	     
    	       System.out.println ("expected elements:"+elc);
    	       System.out.println ("populated elements:"+pop);
    	       System.out.println ("size:"+sz);
    	       System.out.println ("estimated false positive probability:"+p);
    	       System.out.println ("number of hashes :"+nf);
    	       String nextLine[] = {s,String.valueOf(cprob), String.valueOf(p),filtername1,String.valueOf(elapsedTimeInSecond)}; 
 			cwr.writeNext(nextLine);  
    	 }
    	 try {
 			cwr.close();
 		} catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
	}
	void load_different_hashf() {
		 BloomLoader ap = new BloomLoader();
		 //configproperties = PropConfig.getInstance();
      //?I will change tnat after initial test
         String cdxfile = properties.get("cdxfile");
         String filtername =  properties.get("filtername");
         String fname =  properties.get("logfile");
         long totlength = 0;
    	 CSVWriter cwr = null;
    	 File resfile = new File(fname);
    		try {
    			cwr = new CSVWriter(new FileWriter(resfile));
    		} catch (IOException e2) {
    			// TODO Auto-generated catch block
    			e2.printStackTrace();
    		}
    	//HashMethod a = HashMethod.MD5;
    	//HashMethod b = HashMethod.MD5;
    	HashMethod[] methods = {HashMethod.MD5,HashMethod.SHA256,HashMethod.SHA384,
    			HashMethod.SHA512,HashMethod.Murmur3KirschMitzenmacher,HashMethod.Murmur3,HashMethod.RNG,HashMethod.CRC32};
    	 int i;
    	 for (i = 0; i < methods.length; i++) { 
    		 HashMethod m = methods[i]; 
    		String s = m.name();
    		System.out.println("hash"+s);
    		String filtername1 = filtername+s;
    		 int numberofelements = Integer.parseInt( (String)properties.get("numberofelements"));
    		 float prob = Float.parseFloat( properties.get("probability"));
    		
    		 BloomFilter<String> br = new FilterBuilder(numberofelements, prob)
 					//BloomFilter<String> bfr = new FilterBuilder()
 					.name(filtername1) // use a distinct name
 					.hashFunction(m)
 					.hashes(5)
 					.redisHost(host) // localhost
 					.redisPort(port) // Default is standard 6379
 					//.addReadSlave(host, port +1) //add slave
 	                //.addReadSlave(host, port +2)
 					.buildBloomFilter();
    		 
    		 long start = System.nanoTime();
 			ap.load_fromFile(cdxfile,br);
 			long diff = System.nanoTime() - start;
 			double elapsedTimeInSecond = (double) diff / 1000000000;
 			System.out.println(elapsedTimeInSecond + " seconds");
          
 			 Double pop = br.getEstimatedPopulation();
   	       int sz = br.getSize();
   	       
   	       int elc = br.getExpectedElements();
   	       double p = br.getEstimatedFalsePositiveProbability();
   	       double nf = br.getHashes();
   	     
   	       System.out.println ("expected elements:"+elc);
   	       System.out.println ("populated elements:"+pop);
   	       System.out.println ("size:"+sz);
   	       System.out.println ("estimated false positive probability:"+p);
   	       System.out.println ("number of hashes :"+nf);
   	       String nextLine[] = {s, String.valueOf(p),filtername1,String.valueOf(elapsedTimeInSecond)}; 
			cwr.writeNext(nextLine);  
    	 }
    	 try {
			cwr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	 void load_multiple(){
		 BloomLoader ap = new BloomLoader();
		 //configproperties = PropConfig.getInstance();
      //?I will change tnat after initial test
         String cdxfile = properties.get("cdxfile");
         String filtername =  properties.get("filtername");
         String fname =  properties.get("logfile");
         long totlength = 0;
    	 CSVWriter cwr = null;
    	 File resfile = new File(fname);
    		try {
    			cwr = new CSVWriter(new FileWriter(resfile));
    		} catch (IOException e2) {
    			// TODO Auto-generated catch block
    			e2.printStackTrace();
    		}
        // int[] hashes = {3,4,5,6,9,12,15,18,21}; 
         int[] hashes = {4,5};
         int i, x; 
         for (i = 0; i < hashes.length; i++) { 
        	  
             // accessing each element of array 
             x = hashes[i]; 
             System.out.print("hash number:"+x + " "); 
             filtername =filtername+x; 
             int numberofelements = Integer.parseInt( (String)properties.get("numberofelements"));
    		 float prob = Float.parseFloat( properties.get("probability"));
    		 //int numbh  = Integer.parseInt( properties.all().get("numberofhashes"));
    			BloomFilter<String> br = new FilterBuilder(numberofelements, prob)
    					//BloomFilter<String> bfr = new FilterBuilder()
    					.name(filtername) // use a distinct name
    					.hashFunction(HashMethod.Murmur3)
    					.hashes(x)
    					.redisHost(host) // localhost
    					.redisPort(port) // Default is standard 6379
    					//.addReadSlave(host, port +1) //add slave
    	                //.addReadSlave(host, port +2)
    					.buildBloomFilter();
    			;
             
    			long start = System.nanoTime();
    			ap.load_fromFile(cdxfile,br);
    			long diff = System.nanoTime() - start;
    			//1 s = 1,000 ms = 1,000,000 µs = 1,000,000,000 ns
    			//1 000 000 000;
    			double elapsedTimeInSecond = (double) diff / 1000000000;
    			System.out.println(elapsedTimeInSecond + " seconds");
             
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
      	       
      	     String nextLine[] = {String.valueOf(x), String.valueOf(p),filtername,String.valueOf(elapsedTimeInSecond)}; 
 			cwr.writeNext(nextLine);   
         } 
         
      
              
		
		
	}
	
	public static void main(String [] args) throws Exception{
	
	Args arg = new Args(args);
	Map arguments = arg.asMap();
	BloomUtility bu = new BloomUtility();
	
	configproperties = PropConfig.getInstance();
	bu.properties.putAll(configproperties.all());
	
	 //int port = arg.getNumber( "port", AggServer.DEFAULT_PORT ).intValue();
     String activity = arg.get("do", "check");
     String fname = arg.get("filtername", "test");
     if (!fname.equals("test")){
    	 bu.properties.put("filtername",fname);
     }
     if (activity.equals("check")) {
    	 
    	 String filtername = (String) bu.properties.get("filtername");
    	 
    	 BloomFilter<String> br = bu.init_filter(filtername);
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
     if (activity.equals("delete")) {
    	 String filtername = (String) bu.properties.get("filtername");
         
         BloomFilter<String> br = bu.init_filter(filtername);
         br.remove();
        
         System.out.println("filter removed"+filtername);
    	 
     }
    
     
     if (activity.equals("reset_clear")) {
    	 String filtername = (String) bu.properties.get("filtername");
         
         BloomFilter<String> br = bu.init_filter(filtername);
         br.clear();
        
         System.out.println("filter removed"+filtername);
    	 
     }
     if (activity.equals("query_benchmark")){
    	 bu.do_query_check(false);
     }
     
     if (activity.equals("query_benchmark_fp")){
    	 bu.do_query_check(true);
     }
     
     if (activity.equals("download_to_json")){
         String filtername = (String) bu.properties.get("filtername");
         String fdir =  (String) bu.properties.get("jsondir");
         BloomFilter<String> br = bu.init_filter(filtername);         
         download_as_json(br, filtername ,fdir );
     }
     
     if (activity.equals("create_in_memerory_and_download_to_json")){
         String filtername =  (String)bu.properties.get("filtername");
         String fdir =  (String)bu.properties.get("jsondir");
         BloomFilter<String> br = bu.init_mem_filter(filtername); 
         
         download_as_json(br, filtername ,fdir );
     }
     if (activity.equals("load_multiple")){
                          bu.load_multiple();
     }
     if (activity.equals("check_optimum_hashes")){
    	  int numberofelements = Integer.parseInt( bu.properties.get("numberofelements"));
		  int bitstouse  = Integer.parseInt( bu.properties.get("numberofbits"));	 		
          System.out.println("optimal hashnumber"+FilterBuilder.optimalK(numberofelements, bitstouse));
         
     }
     if (activity.equals("check_optimum_size")){
   	  int numberofelements = Integer.parseInt( bu.properties.get("numberofelements"));
   	  float prob = Float.parseFloat( bu.properties.get("probability"));
	  System.out.println("optimal size:"+FilterBuilder.optimalM(numberofelements, prob));
         
    }
     if (activity.equals("check_optimum_probability")){
      	  int numberofelements = Integer.parseInt( bu.properties.get("numberofelements"));
      	  float prob = Float.parseFloat( bu.properties.get("probability"));
      	int bitstouse  = Integer.parseInt( bu.properties.get("numberofbits"));
      	 int k  = Integer.parseInt( bu.properties.get("numberofhashes"));
   		   		
            System.out.println("optimal prob:"+FilterBuilder.optimalP(k, bitstouse, numberofelements));
            
          
            
       }
     
     if (activity.equals("check_params")) {
    		 
    		  int numberofelements = Integer.parseInt( bu.properties.get("numberofelements"));
    		    int bitstouse  = Integer.parseInt( bu.properties.get("numberofbits"));	 
    			BloomFilter<String> bf2 = new FilterBuilder()
    		            .expectedElements(numberofelements) //elements
    		            .size(bitstouse) //bits to use
    		            .hashFunction(HashMethod.Murmur3) //our hash
    		            .buildBloomFilter();
    			
    		 
     }	
   if (activity.contentEquals("qtest")) {
	   bu.do_over();
     
	}
	 if (activity.contentEquals("load_diff_h")) {
	    bu.load_different_hashf();
	 }
	 if (activity.contentEquals("load_diff_p")) {
	       bu.load_different_prob();
	 }
	}
}

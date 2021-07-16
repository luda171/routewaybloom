package gov.lanl.bloom;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;

public class BloomTester {
	 public static void main(String[] args) {
		 PropConfig properties = PropConfig.getInstance();
		 String filterName =  properties.all().get("filtername");

		 int numberofelements = Integer.parseInt( properties.all().get("numberofelements"));
		 float prob = Float.parseFloat( properties.all().get("probability"));
		 int numbh  = Integer.parseInt( properties.all().get("numberofhashes"));
		
		 BloomFilter<String> bfr2 = new FilterBuilder(numberofelements, prob)
				    .name(filterName) //load the same filter
				    .redisBacked(true)
				    .buildBloomFilter();
		 System.out.print(bfr2.contains("01portal.hr/rugvickim-umirovljenicima-300-kuna-bozicnice/"));
	 }
}

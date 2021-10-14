package gov.lanl.bloom.memory.one;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.google.gson.JsonElement;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import gov.lanl.bloom.Args;
import gov.lanl.bloom.PropConfig;
import gov.lanl.bloom.utils.JsonUtil;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.json.BloomFilterConverter;

/**
 * This class take one cdx file and save it as bloomfilter in csv file!
 *
 */
public class BloomWriter {

	static HashMap properties = new HashMap();

	static public BloomFilter<String> init_filter(String filtername) {
		int numberofelements = Integer.parseInt((String) properties.get("numberofelements"));
		float prob = Float.parseFloat((String) properties.get("probability"));
		int numbh = Integer.parseInt((String) properties.get("numberofhashes"));
		String redis = (String) properties.get("redis");
		BloomFilter<String> bfr = null;
		if (redis.equals("true")) {
			bfr = new FilterBuilder(numberofelements, prob).name(filtername).redisBacked(true)
					// use a distinct name
					.hashFunction(HashMethod.Murmur3).hashes(numbh).buildBloomFilter();
		} else {
			bfr = new FilterBuilder(numberofelements, prob).name(filtername)
					// use a distinct name
					.hashFunction(HashMethod.Murmur3).hashes(numbh).buildBloomFilter();
		}
		return bfr;
	}

	int load_cdxfile(String Fname, BloomFilter bf) {

		int tcount = 0;
		CSVParser csvParser = null;
		try {
			FileReader reader = new FileReader(Fname);
			csvParser = new CSVParser(reader,
					CSVFormat.DEFAULT.withDelimiter(' ').withIgnoreEmptyLines().withQuote(null));

			for (CSVRecord record : csvParser) {
				String url = record.get(0).trim();
				// String url = line[0].trim();
				if (url.contains("warc.gz"))
					continue;
				if (url.length() < 2)
					continue;

				bf.add(url);
				tcount = tcount + 1;

			} // for

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {

				csvParser.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return tcount;
	}

	void do_query_check(Boolean neg, BloomFilter br) {
		String cdxfile = (String) properties.get("cdxfile");
		CSVParser csvParser = null;
		CSVReader wr = null;
		boolean check = false;
		BloomFilter<String> dup = init_filter("dtest");

		DescriptiveStatistics stats = new DescriptiveStatistics();
		long tot = 0;
		long ncount = 0;
		long pcount = 0;

		try {

			FileReader reader = new FileReader(cdxfile);
			csvParser = new CSVParser(reader,
					CSVFormat.DEFAULT.withDelimiter(' ').withIgnoreEmptyLines().withQuote(null));
			for (CSVRecord record : csvParser) {
				String url = record.get(0).trim();

				if (url.contains("warc.gz"))
					continue;
				if (url.length() < 2)
					continue;
				//if (dup.contains(url)) {
				//	continue;
				//} else {
				//	dup.add(url);
				//}
				// neg==true means urls are checked not existed
				if (neg)
					url = url + "<>%";
				tot = tot + 1;
				long start = System.nanoTime();
				if (br.contains(url)) {
					pcount = pcount + 1;
				} else {
					ncount = ncount + 1;
				}
				long diff = System.nanoTime() - start;
				// double elapsedTimeInSecond = (double) diff / 1_000_000_000;
				double difference = (diff) / 1e6; // Milliseconds

				stats.addValue(difference);
			}
			System.out.println("test"+neg);
			System.out.print("positive" + pcount);
			System.out.print("negative" + ncount);
			System.out.print("total" + tot);

			// cwr.close();
			System.out.println("mean in Milliseconds" + stats.getMean());
			System.out.println("population variance in Milliseconds" + stats.getPopulationVariance());
			System.out.println("standart deviation in Milliseconds" + stats.getStandardDeviation());
			System.out.println(
					"population standart deviation in Milliseconds" + Math.sqrt(stats.getPopulationVariance()));
			dup.remove();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {

				csvParser.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		// System.out.println("Hello World!");
		Args arg = new Args(args);
		Map arguments = arg.asMap();
		String jdir = arg.get("dirtounload", ".");

		BloomWriter ap = new BloomWriter();

		String cdxfile = arg.get("cdxfile", "test");
		String filtername = arg.get("filtername", "test");
		String numberofelements = arg.get("numberofelements", "200000000");
		String probability = arg.get("probability", "0.05");
		String numberofhashes = arg.get("numberofhashes", "5");
		String archivename = arg.get("archive", "haw");
		String redis = arg.get("redis", "false");
		properties.put("probability", probability);
		properties.put("numberofhashes", numberofhashes);
		properties.put("numberofelements", numberofelements);
		properties.put("redis", redis);
		properties.put("cdxfile", cdxfile);
		BloomFilter<String> br = init_filter(filtername);
		long start = System.nanoTime();
		int c = ap.load_cdxfile(cdxfile, br);

		long diff = System.nanoTime() - start;
		double elapsedTimeInSecond = (double) diff / 1000000000;

		System.out.println("number of recods" + c);
		System.out.println("time to load to bloom:" + elapsedTimeInSecond + " seconds");
		ap.do_query_check(false, br);
		ap.do_query_check(true, br);
		
		JsonUtil ju = new JsonUtil();
		long start0 = System.nanoTime();
		ju.download_as_csv(br, filtername, jdir, filtername, archivename);

		long diff1 = System.nanoTime() - start0;
		double elapsedTimeInSecond1 = (double) diff1 / 1000000000;
		System.out.println("time to serialize from memory to csv:" + elapsedTimeInSecond1 + " seconds");

	}
}

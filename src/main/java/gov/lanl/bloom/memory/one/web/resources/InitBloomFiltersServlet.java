package gov.lanl.bloom.memory.one.web.resources;

import java.io.FileReader;
import java.util.Base64;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import gov.lanl.bloom.memory.one.web.BloomLookupServer;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.memory.BloomFilterMemory;




public class InitBloomFiltersServlet implements ServletContextListener {
	
	
	ServletContext context;
	
	private static InitBloomFiltersServlet _instance;
	private static Map<String, String> conf = BloomLookupServer.INSTANCE.prop;

	public BloomFilter loadcsv(String pathFile) {
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

				FilterBuilder builder = new FilterBuilder(m, k).hashFunction(HashMethod.Murmur3).name(fn);
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
	
	public void contextInitialized(ServletContextEvent sce) {
		// TODO Auto-generated method stub
		this.context = sce.getServletContext();
		_instance = this;
		//load_filters();
		String bfile = conf.get("bloomfile");
		BloomFilter filter=loadcsv(bfile);
		context.setAttribute("filter", filter);
		
		
	}

	public static InitBloomFiltersServlet getInstance() {
		return _instance;
	}
	public Object getAttribute(String key) {
		Object value = context.getAttribute(key);
		return value;
	}
	public void contextDestroyed(ServletContextEvent sce) {
		// TODO Auto-generated method stub
		
	}
    
}

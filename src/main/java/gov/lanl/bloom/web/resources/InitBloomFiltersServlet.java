package gov.lanl.bloom.web.resources;

import java.util.HashMap;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;




public class InitBloomFiltersServlet implements ServletContextListener {
	int numberofelements=200000000;
	float prob = 0.01f;
    int numbh  = 5;
    
    char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    
    static String host = "localhost";
	static int port = 6379;

	static HashMap filters = new HashMap();
	ServletContext context;
	private static InitBloomFiltersServlet _instance;
	public void load_filters(){
		
		 for (int i =0;i < hexArray.length;i++)
	        {
	          String fn= "haw" + hexArray[i];
	          
	          BloomFilter<String> br = new FilterBuilder(numberofelements, prob)
	  				//BloomFilter<String> bfr = new FilterBuilder()
	  				.name(fn) // use a distinct name
	  				.hashFunction(HashMethod.Murmur3)
	  				.hashes(numbh)
	  				.redisHost(host) // localhost
	  				.redisPort(port) // Default is standard 6379
	  				//.addReadSlave(host, port +1) //add slave
	                //.addReadSlave(host, port +2)
	  				.buildBloomFilter();
	          filters.put(fn,br);
	        }
	}

	public void contextInitialized(ServletContextEvent sce) {
		// TODO Auto-generated method stub
		this.context = sce.getServletContext();
		_instance = this;
		load_filters();
		context.setAttribute("filters", filters);
		
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

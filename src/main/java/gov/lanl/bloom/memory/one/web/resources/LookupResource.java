package gov.lanl.bloom.memory.one.web.resources;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.apache.commons.httpclient.URIException;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.archive.wayback.util.url.KeyMakerUrlCanonicalizer;


import orestes.bloomfilter.BloomFilter;


@Path("")
public class LookupResource {
	
	static BloomFilter filter;
	 static {
		    System.out.println("lookupresource init");
	        InitBloomFiltersServlet cl = InitBloomFiltersServlet.getInstance();
	        filter =   (BloomFilter) cl.getAttribute("filter");
	 }
	
    @GET
	@Path("col/{id:.*}")
	//@Produces("application/json")
	public Response lookup(@PathParam("id") String url) {
		System.out.println(url);
		 
		 UrlCanonicalizer can  =	new AggressiveUrlCanonicalizer();
		// KeyMakerUrlCanonicalizer can2= new   KeyMakerUrlCanonicalizer();
		 String normurl= url;
		 try {
			 normurl = can.urlStringToKey(url);
			//String normurl2 = can2.urlStringToKey(url);
		} catch (URIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return lookupnorm( normurl);
		
	}
    @HEAD
   	@Path("col/{id:.*}")
   	//@Produces("application/json")
   	public Response hlookup(@PathParam("id") String url) {
   		System.out.println(url);
   		 
   		 UrlCanonicalizer can  =	new AggressiveUrlCanonicalizer();
   		// KeyMakerUrlCanonicalizer can2= new   KeyMakerUrlCanonicalizer();
   		 String normurl= url;
   		 try {
   			 normurl = can.urlStringToKey(url);
   			//String normurl2 = can2.urlStringToKey(url);
   		} catch (URIException e) {
   			// TODO Auto-generated catch block
   			e.printStackTrace();
   		}
   		 return lookupnorm( normurl);
   		
   	}
    
	@GET
	@Path("col/norm/{id:.*}")
	//@Produces("application/json")
	public Response lookupnorm(@PathParam("id") String normurl) {
		System.out.println(normurl);
	      
	     if (filter.contains(normurl)) {
		 return Response.status(200).build(); }
	     else {
	    	 return Response.status(404).build(); 
	     }
		
	}
	
	@HEAD
	@Path("col/norm/{id:.*}")
	//@Produces("application/json")
	public Response hlookupnorm(@PathParam("id") String normurl) {
		System.out.println(normurl);
	     //BloomRecorderUtil br = new BloomRecorderUtil();
	    // String ha = br.getMd5(normurl);
	     
	     
	     
	    
	     if (filter.contains(normurl)) {
		 return Response.status(200).build(); }
	     else {
	    	 return Response.status(404).build(); 
	     }
		
	}
	
	public static void main(String[] argsArray) throws Exception {
		String u1="https://0.allegroimg.com/s512/03f6f3/9945cfa44deaa364f10422119e00/Drzwi-tylne-tyl-prawe-MAZDA-TRIBUTE-01-07";
		String u2="http://0.gravatar.com/avatar/0c6236f692d226752006d1073cf9d2a7?s=60&d=http%3A%2F%2Fwww.omg.hr%2Fwp-content%2Fthemes%2Fomg%2Fimages%2Fglobal%2Fimg-default-grav.png%3Fs%3D60&r=G";
		String r="0.gravatar.com/avatar/0c6236f692d226752006d1073cf9d2a7?s=60&d=http://www.omg.hr/wp-content/themes/omg/images/global/img-default-grav.png?s=60&r=g";
		 UrlCanonicalizer can  =	new AggressiveUrlCanonicalizer();
		 //KeyMakerUrlCanonicalizer can2= new   KeyMakerUrlCanonicalizer();
		 try {
			String normurl = can.urlStringToKey(u2);
			//String normurl2 = can2.urlStringToKey(u1);
			System.out.println(normurl);
			if (normurl.equals(r)) System.out.println(true);
			//System.out.println(normurl2);
		} catch (URIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

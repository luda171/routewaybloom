package gov.lanl.bloom.memory.one.web;

public class BloomMain {
	 public static void main(String[] argsArray) throws Exception {
	        InputArgs args = new InputArgs( argsArray );
	       
	        
	        int port = args.getNumber( "port", BloomLookupServer.DEFAULT_PORT ).intValue();
	        String sdir = args.get("csvbloomfile", "/var/www");
	        System.out.println("sdir" + sdir);
	        final String baseUri = BloomLookupServer.getLocalhostBaseUri( port );
	        System.out.println(String.format("Running server at [%s]", baseUri));
	      //  ArchiveConfig.loadConfigFile();
	         BloomLookupServer.INSTANCE.startServer( port,sdir );
	        System.out.println("Press Ctrl-C to kill the server");
	        
	       
	       
	        Runtime.getRuntime().addShutdownHook( new Thread()
	        {
	            @Override public void run()
	            {
	                try
	                {
	                  //  System.out.println( "Shutting down the server" );
	                   BloomLookupServer.INSTANCE.stopServer();
	                 
	                }
	                catch ( Exception e )
	                {
	                    throw new RuntimeException( e );
	                }
	            }
	        } );
	    
	 try {
		 System.out.println(String.format("Jersey app started with WADL available at "
              + "%sapplication.wadl\nTry out %stest\nHit enter to stop it...",
              baseUri, baseUri));
	         System.in.read();
	 }
	 finally {
      BloomLookupServer.INSTANCE.stopServer();
  }
} 
}

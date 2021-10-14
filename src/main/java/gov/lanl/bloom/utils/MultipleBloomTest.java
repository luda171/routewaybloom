package gov.lanl.bloom.utils;

import java.io.BufferedReader;
import java.util.Map;

import gov.lanl.bloom.Args;

public class MultipleBloomTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Args arg = new Args(args);
		Map arguments = arg.asMap();
		BufferedReader reader = null;
		String fname = arg.get("cdxfilename", "test");
		
		MultipleBloomLoader mb = new MultipleBloomLoader();
	    mb.do_query_check(true,fname);
	    mb.do_query_check(false,fname);
	}

}

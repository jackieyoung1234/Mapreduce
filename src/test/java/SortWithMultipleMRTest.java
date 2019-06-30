import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;

public class SortWithMultipleMRTest {
    private static final String TEST_INPUT_PATH = "unittest_input";
    private static final String TEST_OUTPUT_PATH = "unittest_output";
    private static final String REDUCER_OUTPUT_PATH = "unittest_mr_output";

    @Before
    public void setUp(){
        new InputDataGenerator(1,10,3).generateFile(false);
        InputDataGenerator ig = new InputDataGenerator(100,99000,9100);
        int[] data = ig.generate();
        ig.generateFile(data,false,TEST_INPUT_PATH+"/in.txt");
        Arrays.sort(data);
        ig.generateFile(data,false,TEST_OUTPUT_PATH+"/expected.txt");
    }
    @Test
    public void test() throws Exception{
        LogsSortWithMultipleMR lsm = new LogsSortWithMultipleMR(TEST_INPUT_PATH,REDUCER_OUTPUT_PATH);
        lsm.run(null);
        //check
        BufferedReader resReader = new BufferedReader(new FileReader(REDUCER_OUTPUT_PATH+"/part-m-00000"));
        BufferedReader expedReader = new BufferedReader(new FileReader(TEST_OUTPUT_PATH+"/expected.txt"));
        String res = resReader.readLine();
        String exped = expedReader.readLine();
        while(res!=null&&exped!=null){
            assertEquals(res.split("\\s+")[5],exped.trim());
            res = resReader.readLine();
            exped = expedReader.readLine();
        }
        resReader.close();
        expedReader.close();
    }
}

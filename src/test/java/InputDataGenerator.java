import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class InputDataGenerator {
    private int lowerBound;
    private int upperBound;
    private int k;
    private Random r;
    private static final String PATH = "input/222.txt";
    private static final String NEWLINE = System.lineSeparator();


    public InputDataGenerator(int l, int h, int k){
       this.lowerBound = l;
       this.upperBound = h;
       this.k = k;
       this.r = new Random();
    }

    public int[] generate(){
       int l = lowerBound;
       int[] result = new int[k];
       int index = 0;
       for(;index<k;){
          result[index++] = l++;
       }
       for(;index<=upperBound-lowerBound;index++){
          int randomIndex =  r.nextInt(index+1);
          if(randomIndex<k) result[randomIndex] = lowerBound+index;

       }
       return result;
    }


    public void generateFile(boolean append){
        try {
            File f = new File(PATH);
            if(!f.exists()){
                f.createNewFile();
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(f,append));
            for(int i: generate()){
               bw.write(String.valueOf(i)+NEWLINE);
            }
            //bw = new BufferedWriter(new FileWriter(f,true));
            bw.flush();
            bw.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
    public void generateFile(int[] data,boolean append,String path){
        try {
            File f = new File(path);
            if(!f.exists()){
                f.createNewFile();
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(f,append));
            for(int i: data){
               bw.write(String.valueOf(i)+NEWLINE);
            }
            //bw = new BufferedWriter(new FileWriter(f,true));
            bw.flush();
            bw.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
    public static void main (String[] args){
        new InputDataGenerator(1,10,3).generateFile(false);
        new InputDataGenerator(100,10000,100).generateFile(false);
        new InputDataGenerator(100000,133302,100).generateFile(true);
    }
}

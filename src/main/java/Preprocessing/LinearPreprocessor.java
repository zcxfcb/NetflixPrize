// Java Program to illustrate reading from FileReader
// using Scanner Class reading entire File
// without using loop
import java.io.File;
    import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.io.Text;

//public class LinearPreprocessor
//{
//  public static void main(String[] args)
//      throws FileNotFoundException
//  {
//    File file = new File("input/raw.txt");
//    Scanner sc = new Scanner(file);
//
//    // we just need to use \\Z as delimiter
//    sc.useDelimiter("\\Z");
//
//    System.out.println(sc.next());
//  }
//}

public class LinearPreprocessor
{
  public  static void main(String[] args) throws Exception {
    String absolutePath = "/Users/Chenxi/Desktop/finalProject/git/NetflixPrize/dataset4";
    try(FileOutputStream fileOutputStream = new FileOutputStream(absolutePath)) {
      String fileContent = "This is a sample text.";
//      fileOutputStream.write(fileContent.getBytes());
      read(fileOutputStream);
    } catch (FileNotFoundException e) {
      // exception handling
    } catch (IOException e) {
      // exception handling
    }
  }
  public static void read(FileOutputStream fileOutputStream) throws Exception
  {
    // pass the path to the file as a parameter
    File file =
        new File("/Users/Chenxi/Downloads/netflixData/combined_data_4.txt");
    Scanner sc = new Scanner(file);
    String Delimiter = ",";
    String userID = "";

    while (sc.hasNextLine()) {
      String valStr = sc.nextLine();
      String[] valStrArr = valStr.split(",");
//      System.out.println("31--------"+valStr);
      if(valStr.charAt(valStr.length()-1) == ':') {
        userID = valStr.substring(0, valStr.length()- 1);
      } else if (valStrArr.length == 3) {
        String movieID = valStrArr[0];
        String rating = valStrArr[1];
        String date = valStrArr[2];
//        System.out.println(userID + Delimiter + movieID + Delimiter + rating);
        fileOutputStream.write((userID + Delimiter + movieID + Delimiter + rating +"\n").getBytes());
      }
      else {
        System.out.println("Invalid line of:" + valStr);
        throw new InterruptedException("Invalid line of:" + valStr);
      }
    }
  }
}
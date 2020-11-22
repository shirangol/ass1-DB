public class Main {

    public static void main(String[] args) {
        Assigment db= new Assigment("jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE","golzar","abcd");
//        db.fileToDataBase("C:\\Users\\shira\\Downloads\\films.csv");
        db.calculateSimilarity();
        db.printSimilarItems(10);

    }
}

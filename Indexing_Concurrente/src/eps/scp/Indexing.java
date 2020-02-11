/* ---------------------------------------------------------------
Práctica 2.
Código fuente : Indexing.java
Grau Informàtica
48257114D   Xavier Berga Puig
21020761A   Jose Almunia Bourgon
--------------------------------------------------------------- */
package eps.scp;

import java.io.IOException;

public class Indexing {

    public static void main(String[] args) throws IOException {
        InvertedIndex hash;

        if (args.length <3 || args.length>5) {
            System.err.println("Error in Parameters. Usage: Indexing <TextFile> <Threads_Number> [<Key_Size>] [<Index_Directory>]");
        }

        if (args.length < 3) {
            hash = new InvertedIndex(args[0]);
        }
        else {
            hash = new InvertedIndex(args[0], Integer.parseInt(args[2]));
        }

        int num_threads = Integer.parseInt(args[1]);
        hash.SetNumThreads(num_threads);
        hash.InitHashTable(num_threads);
        hash.BuildIndex();

        if (args.length > 3) {
            hash.SaveIndex(args[3]);
        }

        else {
            hash.PrintIndex();
        }
        //Liberar memoria
        System.gc();
    }
}

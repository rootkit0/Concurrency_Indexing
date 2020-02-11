/* ---------------------------------------------------------------
Práctica 2.
Código fuente : LoadIndexThread.java
Grau Informàtica
48257114D   Xavier Berga Puig
21020761A   Jose Almunia Bourgon
--------------------------------------------------------------- */
package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.*;

public class LoadIndexThread extends Thread implements Runnable {

    private File[] listOfFiles;
    private int init_file;
    private int final_file;
    private HashMultimap<String, Long> Hash;

    public LoadIndexThread(File[] listOfFiles, int init_file, int final_file, HashMultimap<String, Long> Hash) {
        this.listOfFiles = listOfFiles;
        this.init_file = init_file;
        this.final_file = final_file;
        this.Hash = Hash;
    }

    @Override
    public void run() {
        for(int i=init_file; i<final_file; ++i) {
            if (listOfFiles[i].isFile()) {
                //System.out.println("Processing file " + folder.getPath() + "/" + file.getName()+" -> ");
                try {
                    FileReader input = new FileReader(listOfFiles[i]);
                    BufferedReader bufRead = new BufferedReader(input);
                    String keyLine = null;
                    try {
                        // Leemos fichero línea a linea (clave a clave)
                        while ((keyLine = bufRead.readLine()) != null) {
                            // Descomponemos la línea leída en su clave (k-word) y offsets
                            String[] fields = keyLine.split("\t");
                            String key = fields[0];
                            String[] offsets = fields[1].split(",");
                            // Recorremos los offsets para esta clave y los añadimos al HashMap
                            for (int j = 0; j < offsets.length; j++) {
                                long offset = Long.parseLong(offsets[j]);
                                Hash.put(key, offset);
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Error reading Index file");
                        e.printStackTrace();
                    }
                } catch (FileNotFoundException e) {
                    System.err.println("Error opening Index file");
                    e.printStackTrace();
                }
            }
        }
    }
}

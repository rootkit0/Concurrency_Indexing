/* ---------------------------------------------------------------
Práctica 2.
Código fuente : BuildIndexThread.java
Grau Informàtica
48257114D   Xavier Berga Puig
21020761A   Jose Almunia Bourgon
--------------------------------------------------------------- */
package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class BuildIndexThread extends Thread implements Runnable {

    private int init_pos;
    private int workPerThread;
    private int KeySize;
    private String InputFilePath;
    private HashMultimap<String, Long> Hash;

    public BuildIndexThread(int init_pos, int workPerThread, int KeySize, String InputFilePath, HashMultimap<String, Long> Hash) {
        this.init_pos = init_pos;
        this.workPerThread = workPerThread;
        this.KeySize = KeySize;
        this.InputFilePath = InputFilePath;
        this.Hash = Hash;
    }

    @Override
    public void run() {
        FileInputStream is;
        try {
            long offset = init_pos - 1;
            int car;
            String key="";
            int num_keys = 0;

            File file = new File(InputFilePath);
            is = new FileInputStream(file);
            is.skip(init_pos);

            while((car = is.read()) != -1 && num_keys != workPerThread) {
                offset++;
                if(car=='\n' || car=='\r' || car=='\t') {
                    // Sustituimos los carácteres de \n,\r,\t en la clave por un espacio en blanco.
                    if (key.length()==KeySize && key.charAt(KeySize-1)!=' ')
                        key = key.substring(1, KeySize) + ' ';
                    continue;
                }
                if(key.length()<KeySize) {
                    // Si la clave es menor de K, entonces le concatenamos el nuevo carácter leído.
                    key = key + (char) car;
                }
                else {
                    // Si la clave es igual a K, entonces eliminaos su primier carácter y le concatenamos el nuevo carácter leído (implementamos una slidding window sobre el fichero a indexar).
                    key = key.substring(1, KeySize) + (char) car;
                }
                if(key.length()==KeySize) {
                    //Incrementamos el numero de claves tratadas por el thread
                    ++num_keys;
                    // Si tenemos una clave completa, la añadimos al Hash, junto a su desplazamiento dentro del fichero.
                    AddKey(key, offset - KeySize + 1);
                }
            }

        } catch (FileNotFoundException fnfE) {
            System.err.println("Error opening Input file.");
        }  catch (IOException ioE) {
            System.err.println("Error read Input file.");
        }
    }

    private void AddKey(String key, long offset){
        Hash.put(key, offset);
        //System.out.print(offset+"\t-> "+key+"\r");
    }
}

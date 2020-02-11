/* ---------------------------------------------------------------
Práctica 2.
Código fuente : InvertedIndex.java
Grau Informàtica
48257114D   Xavier Berga Puig
21020761A   Jose Almunia Bourgon
--------------------------------------------------------------- */
package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.*;
import java.util.*;

/**
 * Created by Nando on 3/10/19.
 */
public class InvertedIndex
{
    // Constantes
    private final int DKeySize = 10;            // Tamaño de la clave/ k-word por defecto.
    private final int DIndexMaxNumberOfFiles = 1000;   // Número máximio de ficheros para salvar el índice invertido.
    private final float DMinimunMatchingPercentage = 0.80f;  // Porcentaje mínimo de matching entre el texto original y la consulta (80%)
    private final int DPaddingMatchText = 20;   // Al mostrar el texto original que se corresponde con la consulta se incrementa en 20 carácteres

    // Members
    private String InputFilePath;       // Contiene la ruta del fichero a Indexar.
    private RandomAccessFile randomInputFile;  // Fichero random para acceder al texto original con mayor porcentaje de matching.
    private int KeySize;            // Número de carácteres de la clave (k-word)

    HashMultimap<String, Long> Hash = HashMultimap.create();  //Hashmultimap comun
    HashMultimap<String, Long> hash[];  //Array de hashmultimap que utilizaremos para guardar los resultados de cada thread

    private int num_threads;

    // Constructores
    public InvertedIndex() {
        InputFilePath = null;
        KeySize = DKeySize;
    }

    public InvertedIndex(String inputFile) {
        this();
        InputFilePath = inputFile;
    }

    public InvertedIndex( int keySize) {
        this();
        KeySize = keySize;
    }

    public InvertedIndex(String inputFile, int keySize) {
        InputFilePath = inputFile;
        KeySize = keySize;
    }

    public void SetFileName(String inputFile) {
        InputFilePath = inputFile;
    }

    public void SetNumThreads(int number_threads) { num_threads = number_threads; }

    public void InitHashTable(int number_threads ) {
        //Set the hash table positions
        hash = new HashMultimap[number_threads];
        //Create the hashmultimap in each position
        for(int i=0; i<number_threads; ++i) {
            hash[i] = HashMultimap.create();
        }
    }

    /* Método para construir el indice invertido, utilizando un HashMap para almacenarlo en memoria */
    public void BuildIndex()
    {
        FileInputStream is;
        try {
            File file = new File(InputFilePath);
            is = new FileInputStream(file);
            //Calcular trabajo
            int work = (int) file.length() - KeySize + 1;
            int workPerThread = work/num_threads;
            //Array de threads
            Thread threads[] = new Thread[num_threads];
            //Inicializo la posicion del primer thread
            int init_pos = 0;

            for(int i=0; i<num_threads; ++i) {
                //Todos los threads excepto el ultimo
                if(i < (num_threads - 1)) {
                    //Ejecutar el thread
                    threads[i] = new BuildIndexThread(init_pos, workPerThread, KeySize, InputFilePath, hash[i]);
                    threads[i].start();
                    //Calcular nueva posicion
                    init_pos = init_pos + workPerThread;
                }
                //Ultimo thread
                else {
                    //Ejecutar el thread
                    threads[i] = new BuildIndexThread(init_pos, workPerThread, KeySize, InputFilePath, hash[i]);
                    threads[i].start();
                }
            }
            is.close();
            //Join de los threads
            for(int i=0; i<num_threads; ++i) {
                try {
                    threads[i].join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //Juntamos el array de hash para evitar repeticiones de las claves
            for(int i=0; i<num_threads; ++i) {
                Hash.putAll(hash[i]);
            }
        } catch (FileNotFoundException fnfE) {
            System.err.println("Error opening Input file.");
        }  catch (IOException ioE) {
            System.err.println("Error read Input file.");
        }
    }

    /*
    public void BuildIndex2() {
        byte[] chunk = new byte[DChunkSize];
        int chunkLen = 0, k = 0, countFChars=0;
        long offset = 0;

        try {
            File file = new File(InputFilePath);
            is = new FileInputStream(file);
            k=chunk.length-1;
            while((chunkLen = ReadChunk(chunk,k))>=KeySize)
            {
                String data = new String(chunk).replaceAll("\n", " ").replaceAll("\r", " ").replaceAll("\t", " ");
                //countFChars = data.length() - data.replaceAll("\n", "").replaceAll("\r", "").replaceAll("\t", "").length();
                //.replace("\n", "").replace("\r", "");;
                for (k=0;k<=(chunkLen-(KeySize)); k++)
                {
                    if (offset==582399)
                        System.out.println("Debug");
                    try {
                        char firstCharacter = data.charAt(k);
                        if (firstCharacter != '\n' && firstCharacter != '\r' && firstCharacter != '\t') {
                            String key = GetKey(data, k);
                            if (key=="lugar de l")
                                System.out.println("Debug");
                            if (key != null)
                                AddKey(key, offset);
                        }
                        offset++;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (FileNotFoundException fnfE) {
            System.err.println("Error opening Input file.");
        }
    }
    */

    /*
    private String GetKey(String data, int position)
    {
        String cleanData = data.substring(position);
        cleanData = cleanData.replace("\n", "").replace("\r", "").replace("\t", "");
        if (cleanData.length()>=KeySize)
            return (cleanData.substring(0, KeySize));
        else
            return null;
    }
    */

    /*
    private int ReadChunk(byte[] chunk, int k){
        try {
            System.arraycopy(chunk, k+1, chunk, 0, chunk.length-(k+1));
            int size = is.read(chunk, chunk.length-(k+1), chunk.length-(chunk.length-(k+1)));
            if (size>0)
                return (size+chunk.length-(k+1));
            else
                return (0);
        } catch (IOException e) {
            System.err.println("Error reading Input file.");
            return(0);
        }
    }
    */
    /*
    // Método que añade una k-word y su desplazamiento en el HashMap.
    private void AddKey(String key, long offset){
        Hash.put(key, offset);
        System.out.print(offset+"\t-> "+key+"\r");
    }
    */

    // Método para imprimir por pantalla el índice invertido.
    public void PrintIndex() {
        for(int i=0; i<num_threads; ++i) {
            Set<String> keySet = hash[i].keySet();
            Iterator keyIterator = keySet.iterator();
            while (keyIterator.hasNext() ) {
                String key = (String) keyIterator.next();
                System.out.print(key + "\t");
                Collection<Long> values = hash[i].get(key);
                for(Long value : values){
                    System.out.print(value+",");
                }
                System.out.println();
            }
        }
    }

    public void SaveIndex(String outputDirectory)
    {
        int numberOfFiles, remainingFiles, filesPerThread;
        long remainingKeys=0;

        //Array de threads
        Thread threads[] = new Thread[num_threads];
        // Calculamos el número total de claves en el hash.
        int totalKeySetSize = 0;
        for(int i=0; i<num_threads; ++i) {
            totalKeySetSize += hash[i].size();
        }

        // Calculamos el número de ficheros a crear en función del número de claves que hay en el array de hash.
        if (totalKeySetSize>DIndexMaxNumberOfFiles)
            numberOfFiles = DIndexMaxNumberOfFiles;
        else
            numberOfFiles = totalKeySetSize;

        remainingKeys = totalKeySetSize;
        remainingFiles = numberOfFiles;

        filesPerThread = numberOfFiles/num_threads;

        int first_file = 1;
        int last_file = filesPerThread;

        for(int i=0; i<num_threads; ++i) {
            //Iterador para la posicion i del array de hash
            Set<String> keySetThread = hash[i].keySet();
            Iterator keyIteratorThread = keySetThread.iterator();

            //Calculamos el numero de claves a guardar en los ficheros que trata el thread
            if(i < (num_threads - 1)) {
                //Ejecutar el thread
                threads[i] = new SaveIndexThread(outputDirectory, first_file, last_file, remainingKeys, remainingFiles, keyIteratorThread, hash[i]);
                threads[i].start();
                    first_file += filesPerThread;
                    last_file = first_file + filesPerThread;
            }
            else {
                last_file = numberOfFiles;
                //Ejecutar el thread
                threads[i] = new SaveIndexThread(outputDirectory, first_file, last_file, remainingKeys, remainingFiles, keyIteratorThread, hash[i]);
                threads[i].start();
            }
            remainingKeys -= keySetThread.size();
            remainingFiles -= filesPerThread;
        }
        //Join de los threads
        for(int i=0; i<num_threads; ++i) {
            try {
                threads[i].join();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //Juntamos el array de hash en un solo hash para evitar repeticiones de las claves
        for(int i=0; i<num_threads; ++i) {
            Hash.putAll(hash[i]);
        }
    }

    /*
    // Método para salvar una clave y sus ubicaciones en un fichero.
    public void SaveIndexKey(String key, BufferedWriter bw)
    {
        try {
            Collection<Long> values = Hash.get(key);
            ArrayList<Long> offList = new ArrayList<Long>(values);
            // Creamos un string con todos los offsets separados por una coma.
            String joined = StringUtils.join(offList, ",");
            bw.write(key+"\t");
            bw.write(joined+"\n");
        } catch (IOException e) {
            System.err.println("Error writing Index file");
            e.printStackTrace();
            System.exit(-1);
        }
    }
    */

    // Método para cargar en memoria (HashMap) el índice invertido desde su copia en disco.
    public void LoadIndex(String inputDirectory)
    {
        File folder = new File(inputDirectory);
        File[] listOfFiles = folder.listFiles();

        //Calcular trabajo
        int workPerThread = listOfFiles.length/num_threads;
        //Inicializo el array de threads
        Thread threads[] = new Thread[num_threads];
        //Inicializo el array de hash
        InitHashTable(num_threads);
        //Inicializo las posiciones del primer thread
        int init_file = 0;
        int final_file = workPerThread;

        for(int i=0; i<num_threads; ++i) {
            if(i < (num_threads - 1)) {
                //Ejecutar el thread
                threads[i] = new LoadIndexThread(listOfFiles, init_file, final_file, hash[i]);
                threads[i].start();
                //Calcular las nuevas posiciones
                init_file += workPerThread;
                final_file = init_file + workPerThread;
            }
            else {
                //Calcular la ultima posicion
                final_file = listOfFiles.length;
                //Ejecutar el thread
                threads[i] = new LoadIndexThread(listOfFiles, init_file, final_file, hash[i]);
                threads[i].start();
            }
        }
        //Join de los threads
        for(int i=0; i<num_threads; ++i) {
            try {
                threads[i].join();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //Juntamos el array de hash en un solo hash para evitar repeticiones de las claves
        for(int i=0; i<num_threads; ++i) {
            Hash.putAll(hash[i]);
        }
    }

    public void Query(String queryString) {
        String queryResult=null;
        Map<Long, Integer> offsetsFreq, sorted_offsetsFreq;

        System.out.println ("Searching for query: "+queryString);

        // Split Query in keys & Obtain keys offsets
        offsetsFreq = GetQueryOffsets(queryString);

        // Sort offsets by Frequency in descending order
        sorted_offsetsFreq = SortOffsetsFreq(offsetsFreq);
        //PrintOffsetsFreq(sorted_offsetsFreq);

        // Show results (offsets>Threshold)
        try {
            // Open original input file for random access.
            randomInputFile = new RandomAccessFile(InputFilePath, "r");
        } catch (FileNotFoundException e) {
            System.err.println("Error opening input file");
            e.printStackTrace();
        }
        int maxFreq = (queryString.length()-KeySize)+1;
        Iterator<Map.Entry<Long, Integer>> itr = sorted_offsetsFreq.entrySet().iterator();
        while(itr.hasNext())
        {
            Map.Entry<Long, Integer> entry = itr.next();
            // Calculamos el porcentaje de matching y si es superior al mínimo requerido imprimimos el resultado (texto en esta posición del fichero original)
            if (((float)entry.getValue()/(float)maxFreq)>=DMinimunMatchingPercentage)
                PrintMatching(entry.getKey(), queryString.length(), (float)entry.getValue()/(float)maxFreq);
            else
                break;
        }

        try {
            randomInputFile.close();
        } catch (IOException e) {
            System.err.println("Error opening input file");
            e.printStackTrace();
        }
    }

    // Obtenemos un Map con todos la frecuencia de aparicioón de los offssets asociados con las keys (k-words)
    // generadas a partir de la consulta
    private Map<Long, Integer> GetQueryOffsets(String query)
    {
        Map<Long, Integer> offsetsFreq = new HashMap<Long, Integer>();
        int queryLenght = query.length();
        // Recorremos todas las keys (k-words) de la consulta
        for (int k=0;k<=(queryLenght-KeySize); k++)
        {
            String key = query.substring(k, k+KeySize);
            // Obtenemos y procesamos los offsets para esta key.
            for (Long offset : GetKeyOffsets(key))
            {
                // Increase the number of occurrences of the relative offset (offset-k).
                Integer count = offsetsFreq.get(offset-k);
                if (count == null)
                    offsetsFreq.put(offset-k, 1);
                else
                    offsetsFreq.put(offset-k, count + 1);
            }
        }
        return offsetsFreq;
    }

    // Obtenes los offsets asociados con una key
    private Collection<Long> GetKeyOffsets(String key) {
        return Hash.get(key);
    }

    // Ordenamos la frecuencia de aparición de los offsets de mayor a menor
    private Map<Long, Integer> SortOffsetsFreq( Map<Long, Integer> offsetsFreq)
    {
        List<Map.Entry<Long, Integer>> list = new LinkedList<Map.Entry<Long, Integer>>(offsetsFreq.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Map.Entry<Long, Integer>>()
        {
            public int compare(Map.Entry<Long, Integer> o1, Map.Entry<Long, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<Long, Integer> sortedMap = new LinkedHashMap<Long, Integer>();
        for (Map.Entry<Long, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }

    // Imprimimos la frecuencia de aparición de los offsets.
    private void PrintOffsetsFreq(Map<Long, Integer> offsetsFreq)
    {
        Iterator<Map.Entry<Long, Integer>> itr = offsetsFreq.entrySet().iterator();
        while(itr.hasNext())
        {
            Map.Entry<Long, Integer> entry = itr.next();
            System.out.println("Offset " + entry.getKey() + " --> " + entry.getValue());
        }
    }

    // Imprimimos el texto de un matching de la consulta.
    // A partir del offset se lee y se imprime tantos carácteres como el tamaño de la consulta + N caracteres de padding.
    private void PrintMatching(Long offset, int length, float perMatching)
    {
        byte[] matchText = new byte[length+DPaddingMatchText];

        try {
            // Nos posicionamos en el offset deseado.
            randomInputFile.seek(offset.intValue());
            // Leemos el texto.
            randomInputFile.read(matchText);
        } catch (IOException e) {
            System.err.println("Error reading input file");
            e.printStackTrace();
        }
        System.out.println("Matching at offset "+offset+" ("+ perMatching*100 + "%): "+new String(matchText));
    }
}

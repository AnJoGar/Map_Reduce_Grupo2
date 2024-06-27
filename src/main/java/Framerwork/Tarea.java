
package Framerwork;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author andjo
 */
public class Tarea {
    private String inputFile;
    private String outputfile;
    private int numNodos;

    private NodoMap mapFunction;
    private MapReduce reduceFunction;
    private Object combinerFunction;
     public Tarea() {
        super();
        inputFile = "";
        outputfile = "";
        numNodos = 0;
        mapFunction = null;
        reduceFunction = null;
        combinerFunction = "";
    }
      public void run() {
        Entrada EntradaArchivo  = new Entrada( this.inputFile);
          Salida SalidaArchivo  = new Salida( this.outputfile);

        Particionador bfm = new Particionador();
         Ordenador O = new Ordenador();
        ArrayList<ParClaveValor> resultado = new ArrayList<>();
        ArrayList<ParClaveValor> Listabuffer  = EntradaArchivo.leerYProcesarArchivo(numNodos);
     if (Listabuffer.isEmpty()){
            Logger.getAnonymousLogger().severe("No se ha podido cargar el archivo");
            return;
        }
        System.out.println("Iniciando proceso de Map");
        for (ParClaveValor tupla : Listabuffer) {
            ArrayList<ParClaveValor> output = new ArrayList<>();
            mapFunction.Map(tupla, output);
            bfm.particionarBuffer(output, this.numNodos);
        }
        System.out.println("Iniciando proceso de Ordenamiento");
        O.ordenarBuffer(bfm.getListaParticionada());
        ArrayList<NodoReduce> listaOrdenada = O.getListaOrdenada();
       
        System.out.println("Iniciando proceso de Reduce");
        for (NodoReduce bufferReducer : listaOrdenada) {
            ArrayList<ParClaveValor> listaTuplasReducer  = bufferReducer.getLstParClaveValor();
            for (ParClaveValor tuplaReducer : listaTuplasReducer ) {
                reduceFunction.reduce(tuplaReducer, resultado);
            }
        }
        System.out.println("Guardando los datos en " + outputfile);
        SalidaArchivo.guardarArchivo(resultado);
    }

    public Tarea(String inputFile, String outputfile, int node, NodoMap mapFunction, MapReduce reduceFunction, Object combinerFunction) {
        this.inputFile = inputFile;
        this.outputfile = outputfile;
        this.numNodos = node;
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.combinerFunction = combinerFunction;
    }

    public String getInputFile() {
        return inputFile;
    }

    public String getOutputfile() {
        return outputfile;
    }

    public int getNode() {
        return numNodos;
    }

    public NodoMap getMapFunction() {
        return mapFunction;
    }

    public MapReduce getReduceFunction() {
        return reduceFunction;
    }

    public Object getCombinerFunction() {
        return combinerFunction;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public void setOutputfile(String outputfile) {
        this.outputfile = outputfile;
    }

    public void setNode(int node) {
        this.numNodos = node;
    }

    public void setMapFunction(NodoMap mapFunction) {
        this.mapFunction = mapFunction;
    }

    public void setReduceFunction(MapReduce reduceFunction) {
        this.reduceFunction = reduceFunction;
    }

    public void setCombinerFunction(Object combinerFunction) {
        this.combinerFunction = combinerFunction;
    }
    
    
}

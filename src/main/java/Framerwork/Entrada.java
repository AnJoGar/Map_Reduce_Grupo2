
package Framerwork;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author andjo
 */
public class Entrada {
       String archivoEntrada;
    

    public Entrada(String inputFile) {
        this.archivoEntrada = "./data/" + inputFile;
        
    }


 public ArrayList<ParClaveValor> leerYProcesarArchivo(int numeroNodos) {
        System.out.println("Examinando archivo...");
        ArrayList<ParClaveValor> listaParClaveValor = new ArrayList<>();
        StringBuilder contenidoArchivo = new StringBuilder();
        try {
            File archivo = new File(archivoEntrada);
            if (archivo.exists()) {
                try (BufferedReader lector = new BufferedReader(new FileReader(archivoEntrada))) {
                    String linea;
                    while ((linea = lector.readLine()) != null) {
                        contenidoArchivo.append(linea.trim()).append(" ");
                    }
                } finally {
                    int hash = contenidoArchivo.toString().hashCode() % numeroNodos;
                    listaParClaveValor.add(new ParClaveValor(hash, contenidoArchivo.toString()));
                }
            } else {
                System.err.println("El archivo no existe: " + archivoEntrada);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return listaParClaveValor;
    }
  
}


package Ejercicios;

import Framerwork.MapReduce;
import Framerwork.Tarea;
import Framerwork.ParClaveValor;
import java.util.ArrayList;
import Framerwork.NodoMap;

/**
 *
 * @author John Fernandez
 */
public class Ejercicio1 {

    static class Map implements NodoMap {

        @Override
        public void Map(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
            String[] palabras = elemento.getValor().toString().split(" ");
            for (String palabra : palabras) {
                String nuevaPalabra = palabra.toLowerCase().replaceAll("[^\\w]", "");
                if (nuevaPalabra.equals("404")) {
                    output.add(new ParClaveValor(nuevaPalabra, 1));
                }
            }
        }
    }

    static class Reduce implements MapReduce {

        @Override
        public void reduce(ParClaveValor tupla, ArrayList<ParClaveValor> output) {
            ArrayList<Integer> lista = (ArrayList<Integer>) tupla.getValor();
            int contador = 0;
            for (Integer valor : lista) {
                contador += valor;
            }
            output.add(new ParClaveValor(tupla.getClave(), contador));
        }
    }

    public static void main(String[] args) {
        Tarea t = new Tarea();
        t.setInputFile("weblog.txt");
        t.setOutputfile("Ejercicio1.txt");
        t.setNode(52);
        t.setMapFunction(new Map());
        t.setReduceFunction(new Reduce());
        t.run();
    }
}

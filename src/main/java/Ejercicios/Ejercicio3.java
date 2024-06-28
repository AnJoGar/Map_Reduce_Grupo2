package Ejercicios;

import Framerwork.MapReduce;
import Framerwork.NodoMap;
import Framerwork.Tarea;
import Framerwork.ParClaveValor;
import java.util.ArrayList;
/**
 *
 * @author Jeampier Segarra
 */
public class Ejercicio3 {

    static class MapHourAccesses implements NodoMap {
          /**
         * Guarda en la lista las horas.
         * @param elemento ParClaveValor
         * @param output  .
         */
        @Override
        public void Map(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
            try {
                String[] logParts = ((String) elemento.getValor()).split(" ");
                for (String part : logParts) {
                    if (part.startsWith("[") && part.contains(":")) {
                        String hour = part.split(":")[1];
                        output.add(new ParClaveValor(hour, 1));
                    }
                }
            } catch (Exception e) {
                System.err.println("Error procesando la línea del log: " + elemento.getValor());
            }
        }
    }

    static class ReduceHourAccesses implements MapReduce {
          /**
         * Cuenta las horas
         *
         * @param elemento ParClaveValor
         * @param output   .
         */
        @Override
        public void reduce(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
            ArrayList<Integer> counts = (ArrayList<Integer>) elemento.getValor();
            int totalAccesses = 0;
            for (Integer count : counts) {
                totalAccesses += count;
            }
            output.add(new ParClaveValor(elemento.getClave(), totalAccesses));
        }
    }

    public static void main(String[] args) {
        Tarea t = new Tarea();
        t.setInputFile("weblog.txt");
        t.setOutputfile("Ejercicio3.txt");
        t.setNode(52);
        t.setMapFunction(new MapHourAccesses());
        t.setReduceFunction(new ReduceHourAccesses());
        t.run();
    }
}
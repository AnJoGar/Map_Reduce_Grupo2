package Ejercicios;

import Framerwork.MapReduce;
import Framerwork.NodoMap;
import Framerwork.Tarea;
import Framerwork.ParClaveValor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author andjo
 */
public class Ejercicio6 {

    public static void main(String[] args) {
        Tarea tarea1 = new Tarea();
        tarea1.setMapFunction(new NodoMap() {
            /**
             * calcula la temperatura en superficie mínima y máxima
             *
             * @param elemento ParClaveValor
             * @param output ArrayList que permite agregar las tuplas que
             * queremos.
             */
            @Override
            public void Map(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                String[] line = elemento.getValor().toString().split(" ");
                List<Double> listNumeros = new ArrayList<>();
                for (String item : line) {
                    String[] lineData = item.split(",");
                    double temp = Double.parseDouble(lineData[8]);
                    listNumeros.add(temp);
                }
                double min = Collections.min(listNumeros);
                double max = Collections.max(listNumeros);
                output.add(new ParClaveValor("min", min));
                output.add(new ParClaveValor("max", max));
            }
        });
        tarea1.setReduceFunction(new MapReduce() {
            @Override
            public void reduce(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                output.add(elemento);
            }
        });
        tarea1.setInputFile("JCMB_last31days.csv");
        tarea1.setOutputfile("Ejercicio6.txt");
        tarea1.setNode(30);
        tarea1.run();
    }
}

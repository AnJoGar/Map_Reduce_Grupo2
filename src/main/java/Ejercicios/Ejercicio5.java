package Ejercicios;

import Framerwork.MapReduce;
import Framerwork.NodoMap;
import Framerwork.ParClaveValor;
import Framerwork.Tarea;

import java.util.ArrayList;


/**
 *
 * @author Kevin Taffur
 */
public class Ejercicio5 {
    public static void main(String[] args) {
        Tarea tarea = new Tarea();
        tarea.setMapFunction(new NodoMap() {

            @Override
            public void Map(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                String[] lines = elemento.getValor().toString().split(" ");
                for (String line : lines) {
                    String[] items = line.split(",");
                    double rainfall = Double.parseDouble(items[5]);
                    if (rainfall > 0) {
                        double relativeHumidity = Double.parseDouble(items[9]);
                        double windChill = Double.parseDouble(items[12]);
                        String result = rainfall + ", " + relativeHumidity + ", " + windChill;
                        output.add(new ParClaveValor("data", result));
                    }
                }
            }
        });

        tarea.setReduceFunction(new MapReduce() {

            @Override
            public void reduce(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                ArrayList<String> list = (ArrayList<String>) elemento.getValor();
                for (String item : list) {
                    output.add(new ParClaveValor(elemento.getClave(), item));
                }
            }
        });

        tarea.setInputFile("JCMB_last31days.csv");
        tarea.setOutputfile("Ejercicio5.txt");
        tarea.setNode(30);
        tarea.run();
    }
}
package Ejercicios;

import Framerwork.MapReduce;
import Framerwork.NodoMap;
import Framerwork.Tarea;
import Framerwork.ParClaveValor;

import java.util.ArrayList;

/**
 *
 * @author Katherine Medina
 */

public class Ejercicio4 {

    public static void main(String[] args) {
        Tarea tarea1 = new Tarea();
        tarea1.setMapFunction(new NodoMap() {
        
            @Override
            public void Map(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                String[] lines = elemento.getValor().toString().split(" ");
                
                for (String line : lines) {
                    String[] parts = line.split(",");

                    if (parts.length >= 13) {
                        String surface_Temp = parts[8];
                        String wind_Chill = parts[12];
                        String result = surface_Temp + ", " + wind_Chill;
                        output.add(new ParClaveValor("Resultado: ", result));
                    }
                }
            }
        });

        tarea1.setReduceFunction(new MapReduce() {

            @Override
            public void reduce(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                @SuppressWarnings("unchecked")
                ArrayList<String> list = (ArrayList<String>) elemento.getValor();
                for (String item : list) {
                    output.add(new ParClaveValor(elemento.getClave(), item));
                }
            }
        });

    
        tarea1.setInputFile("JCMB_last31days.csv");
        tarea1.setOutputfile("Ejercicio4.txt");

        tarea1.setNode(30);

        tarea1.run();
    }
}
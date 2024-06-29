/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Ejercicios;



import Framerwork.Particionador;
import Framerwork.MapReduce;
import Framerwork.Tarea;
import Framerwork.ParClaveValor;
import java.util.ArrayList;
import Framerwork.NodoMap;
/**
 *
 * @author andjo
 */
public class Ejercicio7 {
    public static void main(String[] args) {
        Tarea tarea = new Tarea();
        
        tarea.setMapFunction(new NodoMap() {

            @Override
            public void Map(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                String[] line = elemento.getValor().toString().split(" ");
                for (String item : line) {
                    String[] lineData = item.split("\\t");
                    try{
                        double happiness_average = Double.parseDouble(lineData[2]);
                        if (happiness_average < 2 && !lineData[4].equals("--")) {
                            output.add(new ParClaveValor("palabras_demasiado_tristes", 1));
                        }
                    }catch (NumberFormatException e){
                        System.err.println("Error parsing happiness_average: " + lineData[2]);
                    }
                }

            }
        });
        
        tarea.setReduceFunction(new MapReduce() {

            @Override
            public void reduce(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                String palabra = (String) elemento.getClave();
                ArrayList<String> palabras_extremadamente_tristes = (ArrayList<String>) elemento.getValor();
                
                int count = palabras_extremadamente_tristes.size();
                output.add(new ParClaveValor(palabra, count));

            }
        });
        
        tarea.setInputFile("happiness.txt");
        tarea.setOutputfile("Ejercicio7.txt");
        tarea.setNode(30);
        tarea.run();
    }
}
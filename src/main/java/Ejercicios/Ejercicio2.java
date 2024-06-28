/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Ejercicios;

import Framerwork.MapReduce;
import Framerwork.NodoMap;
import Framerwork.Tarea;
import Framerwork.ParClaveValor;
import java.util.ArrayList;
/**
 *
 * @author andjo
 */
public class Ejercicio2 {
    
static class GifMap implements NodoMap {
        /**
         * Guarda en la lista output los ".gif" con un valor 1
         *
         * @param tuplaEntrada ParClaveValor
         * @param listaSalida   ArrayList que permite agregar las tuplas que queremos.
         */
        @Override
        public void Map(ParClaveValor tuplaEntrada, ArrayList<ParClaveValor> listaSalida) {
            String[] palabras = tuplaEntrada.getValor().toString().split(" ");
            for (String palabra : palabras) {
                if (palabra.toLowerCase().contains(".gif")) {
                    listaSalida.add(new ParClaveValor("gif", 1));
                }
            }
        }
    }

    static class SumReduce implements MapReduce {
        /**
         * Cuenta cuántos ".gif" hay en la lista de tuplas ordenadas.
         *
         * @param tuplaClaveValores ParClaveValor que contiene la clave y la lista de valores asociados.
         * @param listaSalida   ArrayList que permite agregar las tuplas que queremos, en la cual será el resultado.
         */
        @Override
        public void reduce(ParClaveValor tuplaClaveValores, ArrayList<ParClaveValor> listaSalida) {
            ArrayList<Integer> listaValores = (ArrayList<Integer>) tuplaClaveValores.getValor();
            int totalGif = listaValores.stream().mapToInt(Integer::intValue).sum();
            listaSalida.add(new ParClaveValor(tuplaClaveValores.getClave(), totalGif));
        }
    }

    public static void main(String[] args) {
        Tarea ejercicio2 = new Tarea();
        ejercicio2.setReduceFunction(new SumReduce());
        ejercicio2.setMapFunction(new GifMap());
        ejercicio2.setInputFile("weblog.txt");
        ejercicio2.setOutputfile("Ejercicio2.txt");
        ejercicio2.setNode(2);
        ejercicio2.run();
    }
}

package Framerwork;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
/**
 *
 * @author andjo
 */
public class NodoReduce {
     private int numeroReducidores;
    private ArrayList<ParClaveValor> listaParClaveValor;

 public NodoReduce(int numeroReducidores, ArrayList<ParClaveValor> listaParClaveValor) {
        this.numeroReducidores = numeroReducidores;
        this.listaParClaveValor = listaParClaveValor;
    }

  
    public int buscarParClaveValorEnLst(ParClaveValor parClaveValor) {
        for (int i = 0; i < listaParClaveValor.size(); i++) {
            String claveTpTmp = ((String) (listaParClaveValor.get(i)).getClave());
            if (claveTpTmp.compareTo((String) parClaveValor.getClave()) == 0) {
                return i;
            }
        }
        return -1;
    }

     public void agregarParCVAlstParClaveValor(ParClaveValor nuevaParClaveValor) {
        int indice = buscarParClaveValorEnLst(nuevaParClaveValor);
        if (indice != -1) {
            ParClaveValor tuplaExistente = listaParClaveValor.get(indice);
            ArrayList<Object> valores = (ArrayList<Object>) tuplaExistente.getValor();
            valores.add(nuevaParClaveValor.getValor());
            listaParClaveValor.set(indice, new ParClaveValor(nuevaParClaveValor.getClave(), valores));
        } else {
            ArrayList<Object> valores = new ArrayList<>();
            valores.add(nuevaParClaveValor.getValor());
            listaParClaveValor.add(new ParClaveValor(nuevaParClaveValor.getClave(), valores));
        }
    }
      public void ejecutarReduce(BiFunction<Object, List<Object>, Object> reduceFunction) {
        ArrayList<ParClaveValor> resultados = new ArrayList<>();
        for (ParClaveValor tupla : listaParClaveValor) {
            String clave = (String) tupla.getClave();
            List<Object> valores = (List<Object>) tupla.getValor();
            Object resultado = reduceFunction.apply(clave, valores);
            resultados.add(new ParClaveValor(clave, resultado));
        }
        listaParClaveValor = resultados;
    }

    public int getNumReducer() {
        return numeroReducidores;
    }

    public ArrayList<ParClaveValor> getLstParClaveValor() {
        return listaParClaveValor;
    } 
}

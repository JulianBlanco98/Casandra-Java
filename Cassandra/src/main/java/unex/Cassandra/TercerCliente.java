package unex.Cassandra;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.Scanner;
import java.util.TreeMap;

public class TercerCliente {

	private Cluster cluster;
	private Session session;

	public Session getSession() {
		return this.session;
	}

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
		session = cluster.connect();
	}

	public void consulta1() {

		session.execute("use ks_julbla_2022");
		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		System.out.println("Cuáles son los lugares más o menos contaminados.");
		System.out.println(
				"\tListando los valores correspondientes a: fecha, hora, nombre del contaminante, valor del contaminante, dirección, longitud y latitud.\n");

		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		// 1. Pedir contaminante

		Scanner sc = new Scanner(System.in);
		System.out.print("Introduce el nombre del contaminante: ");
		String nombreContaminante = sc.nextLine();

		// 2. Pasar a mayúsuclas y borrar los espacios del nombre introducido

		nombreContaminante = convertirMayusculas(nombreContaminante);
		nombreContaminante = eliminarAcentos(nombreContaminante);
		nombreContaminante = eliminarEspacios(nombreContaminante);

		System.out.print("Nombre contaminante corregido: " + nombreContaminante + "\n");

		// 3. Buscar a qué magnitud esta asociado el nombre del Contaminante
		// introducido. Si no está, acaba la función

		int magnitud = getMagnitud(nombreContaminante);

		if (magnitud == -1) {
			System.out.println("El nombre del Contaminante introducido no existe");
		} else {

			// 4. Buscar el máximo

			System.out.println("Magnitud: " + magnitud);

			LinkedList<Float> maximos = new LinkedList<Float>();
			for (int i = 0; i < 24; i++) {

				String dia = String.format("%02d", i + 1);
				ResultSet result = session.execute("SELECT MAX(h" + (dia) + ") FROM tabla3 WHERE magnitud = " + magnitud
						+ " and v" + (dia) + " = 'V' ALLOW FILTERING;");
				Row row = result.one();
				maximos.add(row.getFloat(0));

			}
			// mostrarLista(maximos);

			int posicionMax = 0;
			if (maximos.isEmpty()) {
				System.out.println("La lista está vacía");

			}
			float maximo = maximos.getFirst(); // primer elemento como máximo inicial
			for (int i = 0; i < maximos.size(); i++) {
				float numero = maximos.get(i);
				if (numero > maximo) {
					maximo = numero;
					posicionMax = i;
				}
			}

			// 5. Buscar el mínimo

			LinkedList<Float> minimos = new LinkedList<Float>();

			for (int i = 0; i < 24; i++) {

				String dia = String.format("%02d", i + 1);
				ResultSet result = session.execute("SELECT MIN(h" + (dia) + ") FROM tabla3 WHERE magnitud = " + magnitud
						+ " and v" + (dia) + " = 'V' ALLOW FILTERING;");
				Row row = result.one();
				minimos.add(row.getFloat(0));
			}

			// mostrarLista(minimos);

			int posicionMin = 0;
			float minimo = minimos.getFirst(); // primer elemento como máximo inicial
			for (int i = 0; i < minimos.size(); i++) {
				float numero = minimos.get(i);
				if (numero < minimo) {
					minimo = numero;
					posicionMin = i;
				}
			}

			// 6. Una vez teniendo la hora (posicionMax y posicionMin) y el minimo y el
			// maximo, mostrar por pantalla ambos
			String max = String.format("%02d", (posicionMax + 1));
			String min = String.format("%02d", (posicionMin + 1));
			System.out.println("ESTE ES EL VALOR MÁXIMO DEL CONTAMINANTE ELEGIDO");

			ResultSet res1 = session.execute("SELECT ano, mes, dia, h" + max
					+ ", nombre_contaminante, direccion, longitud, latitud FROM tabla3 WHERE magnitud = " + magnitud
					+ " and h" + max + "= " + maximo + "ALLOW FILTERING;");

			Iterator<Row> it = res1.iterator();

			imprimirResultados(it, max);

			System.out.println("\nESTE ES EL VALOR MÍNIMO DEL CONTAMINANTE ELEGIDO");
			ResultSet res2 = session.execute("SELECT ano, mes, dia, h" + min
					+ ", nombre_contaminante, direccion, longitud, latitud FROM tabla3 WHERE magnitud = " + magnitud
					+ " and h" + min + "= " + minimo + "ALLOW FILTERING;");
			Iterator<Row> it2 = res2.iterator();

			imprimirResultados(it2, min);

		}

	}

	public void consulta4() {
		// TODO Auto-generated method stub

		session.execute("use ks_julbla_2022");

		System.out.println("Según la magnitud introducida, ver todos los valores nulos del contaminante");

		// 1. Pedir contaminante

		Scanner sc = new Scanner(System.in);
		System.out.print("Introduce la magnitud contaminante: ");
		int magnitud = sc.nextInt();

		if (magnitud == -1) {
			System.out.println("Contaminante no está");
		} else {

			System.out.println("Magnitud correcta: ");

			long contador = 0;

			
			//Recorro los 31 dias
			for (int i = 0; i < 31; i++) {

				String dia = String.format("%02d", i + 1);
				ResultSet result = session.execute("SELECT COUNT(*) FROM tablaExamen WHERE magnitud = " + magnitud
						+ " and V" + (dia) + " = 'N' ALLOW FILTERING;"); //El select es resepcto a la nueva tabla
				Row row = result.one();
				long cons= row.getLong(0);
				contador = contador + cons; //el resultado lo sumo al anterior
			}
			
			System.out.println("Número de valores nulos del contaminante: "+magnitud+" : "+contador);

		}

	}

	public void consulta2() {

		session.execute("use ks_julbla_2022");
		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		System.out.println("Cuáles son los lugares cuya contaminación supera o es inferior a ciertos valores");
		System.out.println(
				"\tEn la consulta 2 se piden lugares (estaciones) donde la contaminación sea mayor a un valor introducido por el usuario, independientemente del contaminante.\n");

		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		Scanner sc = new Scanner(System.in);
		System.out.print("Introduce el valor: ");
		float valor = sc.nextFloat();

		while (valor < 0) {
			System.out.print("Error. Introduce un numero positivo: ");
			valor = sc.nextFloat();
		}
		HashMap<Integer, List<Float>> medias = new HashMap<Integer, List<Float>>();

		for (int i = 0; i < 24; i++) {

			String dia = String.format("%02d", i + 1);
			ResultSet result = session
					.execute("SELECT AVG(h" + dia + ") as mediaContaminate, estacion from tabla3 WHERE h" + dia + " > "
							+ valor + " AND v" + dia + " = 'V' GROUP BY estacion ALLOW FILTERING ;");
			for (Row row : result) {
				float mediaContaminate = row.getFloat("mediaContaminate");
				int estacion = row.getInt("estacion");
				agregarValores(medias, estacion, mediaContaminate);
			}

		}
		System.out.println("ESTOS SON LAS MEDIAS DE LOS LUGARES QUE SUPERAN EL NUMERO: " + valor + "\n");
		System.out.printf("%-30s%-30s%n", "Estacion", "Media");
		System.out.println();

		for (Map.Entry<Integer, List<Float>> entry : medias.entrySet()) {
			int clave = entry.getKey();
			List<Float> valores = entry.getValue();
			float suma = 0.0f;
			for (float dato : valores) {
				suma += dato;
			}
			float media = suma / valores.size();
			System.out.printf("%-30s%-30s%n", obtenerValorColumnaDerecha(clave), media);
		}
	}

	public void consulta3() {

		session.execute("use ks_julbla_2022");
		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		System.out.println("En la consulta 3, qué contaminación hay en cada zona");
		System.out.println(
				"\tSería listar los valores máximos (por agrupar) de los contaminantes indicados de cada zona. Entender cada zona como punto de muestreo del CSV."
						+ "buscar los 4 máximos valores de los contaminantes (SO2, CO, NO2, PM2.5) (magnitud) de cada estacion\n");

		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		// Primero: Almacenar cada estación, los valores máximos de los 4 contaminantes

		HashSet<Integer> e1 = new HashSet<Integer>(); // HASHSET para guardar las estaciones que contengan cada
														// contaminante
		HashSet<Integer> e2 = new HashSet<Integer>();
		HashSet<Integer> e3 = new HashSet<Integer>();
		HashSet<Integer> e4 = new HashSet<Integer>();
		TreeMap<Integer, List<Float>> compuesto1 = new TreeMap<Integer, List<Float>>();// TREESET para almacenar los
																						// valores máximos de cada
																						// estación de cada contaminante
		TreeMap<Integer, List<Float>> compuesto2 = new TreeMap<Integer, List<Float>>();
		TreeMap<Integer, List<Float>> compuesto3 = new TreeMap<Integer, List<Float>>();
		TreeMap<Integer, List<Float>> compuesto4 = new TreeMap<Integer, List<Float>>();

		// Segundo: Rellenar cada TreeSet con todos los valores máximos de cada hora de
		// cada contaminantes en las diferentes estaciones.
		// A su vez, rellenar cada HashSet para saber en que estaciones está el
		// contaminante

		for (int i = 0; i < 24; i++) {

			String dia = String.format("%02d", i + 1);
			ResultSet result1 = session
					.execute("SELECT MAX(h" + dia + ") as maximo, estacion FROM tabla3 WHERE magnitud = 1 and v" + dia
							+ " = 'V' GROUP BY estacion ALLOW FILTERING ;");
			for (Row row : result1) {
				float maximo = row.getFloat("maximo");
				int estacion = row.getInt("estacion");
				e1.add(estacion);
				agregarValores(compuesto1, estacion, maximo);
			}

			ResultSet result2 = session
					.execute("SELECT MAX(h" + dia + ") as maximo, estacion FROM tabla3 WHERE magnitud = 6 and v" + dia
							+ " = 'V' GROUP BY estacion ALLOW FILTERING ;");
			for (Row row : result2) {
				float maximo = row.getFloat("maximo");
				int estacion = row.getInt("estacion");
				e2.add(estacion);
				agregarValores(compuesto2, estacion, maximo);

			}

			ResultSet result3 = session
					.execute("SELECT MAX(h" + dia + ") as maximo, estacion FROM tabla3 WHERE magnitud = 8 and v" + dia
							+ " = 'V' GROUP BY estacion ALLOW FILTERING ;");
			for (Row row : result3) {
				float maximo = row.getFloat("maximo");
				int estacion = row.getInt("estacion");
				e3.add(estacion);
				agregarValores(compuesto3, estacion, maximo);
			}

			ResultSet result4 = session
					.execute("SELECT MAX(h" + dia + ") as maximo, estacion FROM tabla3 WHERE magnitud = 9 and v" + dia
							+ " = 'V' GROUP BY estacion ALLOW FILTERING ;");
			for (Row row : result4) {
				float maximo = row.getFloat("maximo");
				int estacion = row.getInt("estacion");
				e4.add(estacion);
				agregarValores(compuesto4, estacion, maximo);
			}
		}

		// Tercero: ArrayList para almacenar todas las estaciones del CSV
		ArrayList<Integer> estaciones = new ArrayList<Integer>();
		ResultSet rs = session.execute("SELECT estacion FROM tabla3 GROUP BY estacion;");
		for (Row row : rs) {
			int estacion = row.getInt("estacion");
			estaciones.add(estacion);
		}

		Collections.sort(estaciones);
		mostrarEstaciones(estaciones);
		System.out.println("-------------------");

		// Cuarto: Listas para guardar los máximos, las horas de cada contaminante y las
		// estaciones donde existe cada contaminante
		// 1.
		LinkedList<Float> maximos1 = new LinkedList<Float>();
		LinkedList<Float> posiciones1 = new LinkedList<Float>();
		maximos1 = obtenerMaximo(compuesto1);
		posiciones1 = obtenerPosicion(compuesto1);
		List<Integer> e1ordenadas = new LinkedList<Integer>(e1);
		Collections.sort(e1ordenadas);
		// 2.
		LinkedList<Float> maximos2 = new LinkedList<Float>();
		LinkedList<Float> posiciones2 = new LinkedList<Float>();
		maximos2 = obtenerMaximo(compuesto2);
		posiciones2 = obtenerPosicion(compuesto2);
		List<Integer> e2ordenadas = new LinkedList<Integer>(e2);
		Collections.sort(e2ordenadas);
		// 3.
		LinkedList<Float> maximos3 = new LinkedList<Float>();
		LinkedList<Float> posiciones3 = new LinkedList<Float>();
		maximos3 = obtenerMaximo(compuesto3);
		posiciones3 = obtenerPosicion(compuesto3);
		List<Integer> e3ordenadas = new LinkedList<Integer>(e3);
		Collections.sort(e3ordenadas);
		// 4.
		LinkedList<Float> maximos4 = new LinkedList<Float>();
		LinkedList<Float> posiciones4 = new LinkedList<Float>();
		maximos4 = obtenerMaximo(compuesto4);
		posiciones4 = obtenerPosicion(compuesto4);
		List<Integer> e4ordenadas = new LinkedList<Integer>(e4);
		Collections.sort(e4ordenadas);

		System.out.println(String.format("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s", "ano",
				"mes", "dia", "estacion", "longitud", "latitud", "Direccion", "Valor", "Nombre contaminante", "Hora"));
		System.out.println(
				"-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");

		boolean encontrado1 = false;
		boolean encontrado2 = false;
		boolean encontrado3 = false;
		boolean encontrado4 = false;
		int cont1 = 0;
		int cont2 = 0;
		int cont3 = 0;
		int cont4 = 0;

		// 5. Recorrer cada estación. Luego, recorrer cada contaminante en esa estación.
		// Si tiene máximo, se muestra por pantalla

		try {
			// Recorro todas las estaciones
			for (int i = 0; i < estaciones.size(); i++) {

				encontrado1 = false;
				encontrado2 = false;
				encontrado3 = false;
				encontrado4 = false;

				// si la estación del primer contaminante coincide con la estación por la que se
				// va buscando
				if (estaciones.get(i) == e1ordenadas.get(cont1)) {

					for (int j = 0; j < maximos1.size(); j++) {
						j = cont1;
						String dia = String.format("%02d", Math.round(posiciones1.get(j)));
						float m = maximos1.get(j);
						// Buscar el máximo ya encontrado antes
						ResultSet contaminante1 = session
								.execute("SELECT ano, mes, dia, estacion, direccion, longitud, latitud, h" + dia
										+ " as valor, magnitud " + "from tabla3 where h" + dia + "=" + m
										+ " and magnitud = 1 and estacion = " + estaciones.get(i)
										+ " group by estacion ALLOW FILTERING");
						Iterator<Row> iterator1 = contaminante1.iterator();
						while (iterator1.hasNext() && !encontrado1) {
							Row row = iterator1.next();
							System.out.printf("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s%n",
									row.getInt("ano"), row.getInt("mes"), row.getInt("dia"), row.getInt("estacion"),
									row.getInt("longitud"), row.getInt("latitud"), row.getString("direccion"),
									row.getFloat("valor"), getAbreviatura(row.getInt("magnitud")), "h" + dia);
							encontrado1 = true;
						}
						if (encontrado1) {
							j = maximos1.size() - 1;
							cont1++;
						}
					}
				}
				if (estaciones.get(i) == e2ordenadas.get(cont2)) {

					for (int j = 0; j < maximos2.size(); j++) {
						j = cont2;
						String dia = String.format("%02d", Math.round(posiciones2.get(j)));
						float m = maximos2.get(j);
						// Buscar el máximo ya encontrado antes
						ResultSet contaminante1 = session
								.execute("SELECT ano, mes, dia, estacion, direccion, longitud, latitud, h" + dia
										+ " as valor, magnitud " + "from tabla3 where h" + dia + "=" + m
										+ " and magnitud = 6 and estacion = " + estaciones.get(i)
										+ " group by estacion ALLOW FILTERING");
						Iterator<Row> iterator1 = contaminante1.iterator();
						while (iterator1.hasNext() && !encontrado2) {
							Row row = iterator1.next();
							System.out.printf("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s%n",
									row.getInt("ano"), row.getInt("mes"), row.getInt("dia"), row.getInt("estacion"),
									row.getInt("longitud"), row.getInt("latitud"), row.getString("direccion"),
									row.getFloat("valor"), getAbreviatura(row.getInt("magnitud")), "h" + dia);
							encontrado2 = true;
						}
						if (encontrado2) {
							j = maximos2.size() - 1;
							cont2++;
						}
					}
				}
				if (estaciones.get(i) == e3ordenadas.get(cont3)) {

					for (int j = 0; j < maximos3.size(); j++) {
						j = cont3;
						String dia = String.format("%02d", Math.round(posiciones3.get(j)));
						float m = maximos3.get(j);
						// Buscar el máximo ya encontrado antes
						ResultSet contaminante1 = session
								.execute("SELECT ano, mes, dia, estacion, direccion, longitud, latitud, h" + dia
										+ " as valor, magnitud " + "from tabla3 where h" + dia + "=" + m
										+ " and magnitud = 8 and estacion = " + estaciones.get(i)
										+ " group by estacion ALLOW FILTERING");
						Iterator<Row> iterator1 = contaminante1.iterator();
						while (iterator1.hasNext() && !encontrado3) {
							Row row = iterator1.next();
							System.out.printf("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s%n",
									row.getInt("ano"), row.getInt("mes"), row.getInt("dia"), row.getInt("estacion"),
									row.getInt("longitud"), row.getInt("latitud"), row.getString("direccion"),
									row.getFloat("valor"), getAbreviatura(row.getInt("magnitud")), "h" + dia);
							encontrado3 = true;
						}
						if (encontrado3) {
							j = maximos3.size() - 1;
							cont3++;
						}
					}
				}
				if (estaciones.get(i) == e4ordenadas.get(cont4)) {

					for (int j = 0; j < maximos4.size(); j++) {
						j = cont4;
						String dia = String.format("%02d", Math.round(posiciones4.get(j)));
						float m = maximos4.get(j);
						// Buscar el máximo ya encontrado antes
						ResultSet contaminante1 = session
								.execute("SELECT ano, mes, dia, estacion, direccion, longitud, latitud, h" + dia
										+ " as valor, magnitud " + "from tabla3 where h" + dia + "=" + m
										+ " and magnitud = 9 and estacion = " + estaciones.get(i)
										+ " group by estacion ALLOW FILTERING");
						Iterator<Row> iterator1 = contaminante1.iterator();
						while (iterator1.hasNext() && !encontrado4) {
							Row row = iterator1.next();
							System.out.printf("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s%n",
									row.getInt("ano"), row.getInt("mes"), row.getInt("dia"), row.getInt("estacion"),
									row.getInt("longitud"), row.getInt("latitud"), row.getString("direccion"),
									row.getFloat("valor"), getAbreviatura(row.getInt("magnitud")), "h" + dia);
							encontrado4 = true;
						}
						if (encontrado4) {
							j = maximos4.size() - 1;
							cont4++;
						}
					}
				}

			}
		} catch (IndexOutOfBoundsException e) {

		}

	}

	private LinkedList<Float> obtenerPosicion(TreeMap<Integer, List<Float>> treeMap) {

		LinkedList<Float> aux = new LinkedList<Float>();

		for (Map.Entry<Integer, List<Float>> entry : treeMap.entrySet()) {
			int posicion = -1;
			float maximo = Float.MIN_VALUE;
			List<Float> valores = entry.getValue();

			for (int i = 0; i < valores.size(); i++) {
				float valor = valores.get(i);
				if (valor > maximo) {
					maximo = valor;
					posicion = i;
				}
			}

			aux.add((posicion + 1f));
		}

		return aux;
	}

	private LinkedList<Float> obtenerMaximo(TreeMap<Integer, List<Float>> treeMap) {

		LinkedList<Float> aux = new LinkedList<Float>();

		for (Map.Entry<Integer, List<Float>> entry : treeMap.entrySet()) {
			int posicion = -1;
			float maximo = Float.MIN_VALUE;
			List<Float> valores = entry.getValue();

			for (int i = 0; i < valores.size(); i++) {
				float valor = valores.get(i);
				if (valor > maximo) {
					maximo = valor;
					posicion = i;
				}
			}

			aux.add(maximo);
		}

		return aux;
	}

	/*
	 * private void mostrarTreeMap(TreeMap<Integer, List<Float>> treeMap) { for
	 * (Integer clave : treeMap.keySet()) { System.out.print("Clave: " + clave +
	 * ", Valores: "); List<Float> valores = treeMap.get(clave); for (Float valor :
	 * valores) { System.out.print(valor + " "); } System.out.println(); } }
	 */
	private void mostrarEstaciones(ArrayList<Integer> estaciones) {

		System.out.print("Estaciones: ");
		Iterator it = estaciones.iterator();
		while (it.hasNext()) {

			System.out.print(it.next() + ", ");
		}
		System.out.println();
	}

	private String getAbreviatura(int magnitud) {
		if (magnitud == 1) {
			return "S02";
		} else if (magnitud == 6) {
			return "C0";
		} else if (magnitud == 8) {
			return "N02";
		} else if (magnitud == 9) {
			return "PM2.5";
		} else {
			return "";
		}
	}

	private static void agregarValores(HashMap<Integer, List<Float>> medias, int estacion, float valor) {
		List<Float> valores = medias.getOrDefault(estacion, new ArrayList<Float>());
		valores.add(valor);
		medias.put(estacion, valores);
	}

	private static void agregarValores(TreeMap<Integer, List<Float>> medias, int estacion, float valor) {
		List<Float> valores = medias.getOrDefault(estacion, new ArrayList<Float>());
		valores.add(valor);
		medias.put(estacion, valores);
	}

	private void mostrarLista(LinkedList<Float> numeros) {

		Iterator it = numeros.iterator();
		System.out.println("Numeros de la lista");
		while (it.hasNext()) {
			System.out.print(it.next() + ", ");
		}

		System.out.println();
	}

	private int getMagnitud(String contaminante) {
		if (contaminante.equals("DIOXIDODEAZUFRE")) {
			return 1;
		} else if (contaminante.equals("MONOXIDODECARBONO")) {
			return 6;
		} else if (contaminante.equals("MONOXIDODENITROGENO")) {
			return 7;
		} else if (contaminante.equals("DIOXIDODENITROGENO")) {
			return 8;
		} else if (contaminante.equals("PARTICULAS<2.5ΜM")) {
			return 9;
		} else if (contaminante.equals("PARTICULAS<10ΜM")) {
			return 10;
		} else if (contaminante.equals("OXIDOSDENITROGENO")) {
			return 12;
		} else if (contaminante.equals("OZONO")) {
			return 14;
		} else if (contaminante.equals("TOLUENO")) {
			return 20;
		} else if (contaminante.equals("BENCENO")) {
			return 30;
		} else if (contaminante.equals("ETILBENCENO")) {
			return 35;
		} else if (contaminante.equals("METAXILENO")) {
			return 37;
		} else if (contaminante.equals("PARAXILENO")) {
			return 38;
		} else if (contaminante.equals("ORTOXILENO")) {
			return 39;
		} else if (contaminante.equals("HIDROCARBUROSTOTALES")) {
			return 42;
		} else if (contaminante.equals("METANO")) {
			return 43;
		} else if (contaminante.equals("HIDROCARBUROSNOMETANICOS")) {
			return 44;
		} else {
			return -1; // Valor por defecto en caso de no encontrar el contaminante
		}
	}

	private String convertirMayusculas(String texto) {
		return texto.toUpperCase();
	}

	private String eliminarEspacios(String texto) {
		return texto.replaceAll("\\s", "");
	}

	private String eliminarAcentos(String texto) {
		String textoSinAcentos = Normalizer.normalize(texto, Normalizer.Form.NFD);
		textoSinAcentos = textoSinAcentos.replaceAll("\\p{M}", "");
		return textoSinAcentos;
	}

	private void imprimirResultados(Iterator<Row> iterator, String hora) {
		boolean encontrado = false;
		System.out.println(String.format("%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-8s\t%-8s", "ano", "mes", "dia",
				"h" + hora + "", "nombre_contaminante", "direccion", "longitud", "latitud"));
		System.out.println("-------------------------------------------------------" + "-------------------------");

		while (iterator.hasNext() && !encontrado) {
			Row row = iterator.next();
			int ano = row.getInt("ano");
			int mes = row.getInt("mes");
			int dia = row.getInt("dia");
			float horaC = row.getFloat("h" + hora);
			String nombreContaminante = row.getString("nombre_contaminante");
			String direccion = row.getString("direccion");
			int longitud = row.getInt("longitud");
			int latitud = row.getInt("latitud");

			System.out.println(String.format("%-4d\t%-4d\t%-4d\t%-4s\t%-4s\t%-4s\t%-8s\t%-8s", ano, mes, dia, horaC,
					nombreContaminante, direccion, longitud, latitud));
			encontrado = true;
		}
	}

	public String obtenerValorColumnaDerecha(int numeroColumnaIzquierda) {
		switch (numeroColumnaIzquierda) {
		case 55:
			return "Urb. Embajada";
		case 50:
			return "Plaza de Castilla";
		case 49:
			return "Parque del Retiro";
		case 60:
			return "Tres Olivos";
		case 16:
			return "Arturo Soria";
		case 11:
			return "Ramón y Cajal";
		case 8:
			return "Escuelas Aguirre";
		case 4:
			return "Plaza de España";
		case 18:
			return "Farolillos";
		case 47:
			return "Mendez Alvaro";
		case 54:
			return "Ensanche de Vallecas";
		case 58:
			return "El Pardo";
		case 27:
			return "Barajas Pueblo";
		case 59:
			return "Juan Carlos I";
		case 36:
			return "Moratalaz";
		case 40:
			return "Vallecas";
		case 38:
			return "Cuatro Caminos";
		case 57:
			return "Sanchinarro";
		case 39:
			return "Barrio del Pilar";
		case 56:
			return "Plaza Elíptica";
		case 17:
			return "Villaverde";
		case 35:
			return "Plaza del Carmen";
		case 48:
			return "Castellana";
		case 24:
			return "Casa de Campo";
		default:
			return "Valor no encontrado";
		}
	}

	public void close() {
		session.close();
		cluster.close();
		System.out.println("\nConnection closed");
	}

}

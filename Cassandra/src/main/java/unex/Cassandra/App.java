package unex.Cassandra;

import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		

		TercerCliente clien3 = new TercerCliente();
		clien3.connect("127.0.0.1");
		Scanner scanner = new Scanner(System.in);
		int opcion;

		do {
			System.out.println("Menu de la Practica");
			System.out.println("----");
			System.out.println("1. Consulta 1");
			System.out.println("2. Consulta 2");
			System.out.println("3. Consulta 3");
			System.out.println("4. Consulta 4");
			System.out.println("5. Salir");
			System.out.print("Ingrese una opcion: ");
			opcion = scanner.nextInt();

			switch (opcion) {
			case 1:
				System.out.println("Ha seleccionado la opción 1");
				clien3.consulta1();
				break;
			case 2:
				System.out.println("Ha seleccionado la opción 2");
				clien3.consulta2();
				break;
			case 3:
				System.out.println("Ha seleccionado la opción 3");
				clien3.consulta3();
				break;
			case 4:
				System.out.println("Ha seleccionado la opción 4");
				clien3.consulta4();
				break;
			case 5:
				System.out.println("Saliendo del menu...");
				break;
			default:
				System.out.println("Opción inválida. Por favor, seleccione una opción válida.");
				break;
			}
			System.out.println();
		} while (opcion != 5);


		clien3.close();

	}
}

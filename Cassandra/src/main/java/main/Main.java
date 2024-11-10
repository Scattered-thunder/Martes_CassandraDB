package main;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public class Main {
	public static void main(String[] args) {
		// Create Keyspace
		String keyspace = "harry";
		try (CqlSession session = CqlSession.builder()
				.addContactPoint(new InetSocketAddress("localhost", 9042))
				.withLocalDatacenter("datacenter1")
				.withKeyspace(keyspace)
				.build()) {
			System.out.println("Connected to Cassandra");
			//Ejercicio_1(session, "Banana Cavendish", "Deposito Pepe");
			//Ejercicio_2_Datos(session);
			//Ejercicio_2(session, "Product A");
			//Ejercicio_3_Datos(session);
			Ejercicio_3(session, 2);
		}
		
	}
	
	public static void Ejercicio_3(CqlSession session, Integer idEnvio) {
	
		String selectQuery = "SELECT * FROM PedidosPorEnvio WHERE idEnvio =?;";
		
		SimpleStatement simpleStatement = SimpleStatement.builder(selectQuery)
				.addPositionalValue(idEnvio)
				.build(); 
		
		ResultSet resultSet = session.execute(simpleStatement);
		
		for(Row row: resultSet) {
			System.out.println("-----------------------------------------------------");
			System.out.println("ID del Envio: " + row.getInt("idEnvio"));
			System.out.println("ID del Pedido: " + row.getInt("idPedido"));
		    System.out.println("Cliente: " + row.getString("nombreCliente"));
		    System.out.println("Fecha: " + row.getInstant("fecha"));
		    System.out.println("-----------------------------------------------------");
		}
	}
	
	public static void Ejercicio_3_Datos(CqlSession session) {

		  String createTableQuery =
              "CREATE TABLE IF NOT EXISTS PedidosPorEnvio (" +
              "idEnvio INT, " +
              "idPedido INT, " +
              "fecha TIMESTAMP, " + 
              "nombreCliente TEXT, " +
              "PRIMARY KEY (idEnvio, idPedido)" +
              ");";

          SimpleStatement statement = SimpleStatement.builder(createTableQuery).build();
            
          try {
			  session.execute(statement);
			  System.out.println("Table created");
			  } catch (Exception e) {
			  System.out.println("Table already exists"); 
			  }
          
          try {
          // Insert mock data into the table
          String insertDataQuery1 = 
              "INSERT INTO PedidosPorEnvio (idEnvio, idPedido, fecha, nombreCliente) VALUES (1, 101, '2024-11-10 10:00:00', 'Client A');";
          String insertDataQuery2 = 
              "INSERT INTO PedidosPorEnvio (idEnvio, idPedido, fecha, nombreCliente) VALUES (1, 102, '2024-11-10 11:00:00', 'Client B');";
          String insertDataQuery3 = 
              "INSERT INTO PedidosPorEnvio (idEnvio, idPedido, fecha, nombreCliente) VALUES (2, 103, '2024-11-10 12:00:00', 'Client C');";
          String insertDataQuery4 = 
              "INSERT INTO PedidosPorEnvio (idEnvio, idPedido, fecha, nombreCliente) VALUES (2, 104, '2024-11-10 13:00:00', 'Client D');";
          String insertDataQuery5 = 
              "INSERT INTO PedidosPorEnvio (idEnvio, idPedido, fecha, nombreCliente) VALUES (3, 105, '2024-11-10 14:00:00', 'Client E');";

          // Execute the insert statements
          session.execute(SimpleStatement.newInstance(insertDataQuery1));
          session.execute(SimpleStatement.newInstance(insertDataQuery2));
          session.execute(SimpleStatement.newInstance(insertDataQuery3));
          session.execute(SimpleStatement.newInstance(insertDataQuery4));
          session.execute(SimpleStatement.newInstance(insertDataQuery5));

          System.out.println("Mock data inserted successfully.");
      } catch (Exception e) {
          e.printStackTrace();
      }
	} 		  
	
	public static void Ejercicio_2(CqlSession session, String nombreProducto) {
		String selectQuery = "SELECT * FROM PedidosPorProducto WHERE nombreProducto = ?;";
		
		SimpleStatement selectStatement = SimpleStatement.builder(selectQuery)
				.addPositionalValue(nombreProducto)
				.build();
		
		ResultSet resultSet = session.execute(selectStatement);
		
		for(Row row: resultSet) {
			System.out.println("-----------------------------------------------------");
			System.out.println("Deposito: " + row.getInt("idpedido"));
			System.out.println("ID del Producto: " + row.getInt("idproducto"));
		    System.out.println("Nombre del Producto: " + row.getString("nombreproducto"));
		    System.out.println("Estado: " + row.getString("estado"));
		    System.out.println("Fecha: " + row.getInstant("fecha"));
		    System.out.println("-----------------------------------------------------");
		}

	}
	
	public static void Ejercicio_2_Datos(CqlSession session) {
		
		  // Creo la Tabla
		
		  String createTableQuery =
                "CREATE TABLE IF NOT EXISTS PedidosPorProducto (" +
                "idProducto INT, " +
                "nombreProducto TEXT, " +
                "idPedido INT, " + 
                "nombreCliente TEXT, " +
                "fecha TIMESTAMP, " + 
                "estado TEXT, " +
                "PRIMARY KEY (nombreProducto, idPedido, idProducto)" +
                ");";

          SimpleStatement statement = SimpleStatement.builder(createTableQuery).build();
            
  		  try {
			  session.execute(statement);
			  System.out.println("Table created");
			  } catch (Exception e) {
			  System.out.println("Table already exists"); 
			  }
  		  
  		  try {
  			 String insertDataQuery1 = 
                 "INSERT INTO PedidosPorProducto (idProducto, nombreProducto, idPedido, nombreCliente, fecha, estado) VALUES (1, 'Product A', 101, 'Client 1', '2024-11-10 10:00:00', 'Shipped');";
             String insertDataQuery2 = 
                 "INSERT INTO PedidosPorProducto (idProducto, nombreProducto, idPedido, nombreCliente, fecha, estado) VALUES (2, 'Product A', 102, 'Client 2', '2024-11-10 11:00:00', 'Pending');";
             String insertDataQuery3 = 
                 "INSERT INTO PedidosPorProducto (idProducto, nombreProducto, idPedido, nombreCliente, fecha, estado) VALUES (3, 'Product B', 103, 'Client 3', '2024-11-10 12:00:00', 'Delivered');";
             String insertDataQuery4 = 
                 "INSERT INTO PedidosPorProducto (idProducto, nombreProducto, idPedido, nombreCliente, fecha, estado) VALUES (4, 'Product A', 104, 'Client 4', '2024-11-10 13:00:00', 'Shipped');";
             String insertDataQuery5 = 
                 "INSERT INTO PedidosPorProducto (idProducto, nombreProducto, idPedido, nombreCliente, fecha, estado) VALUES (5, 'Product C', 105, 'Client 5', '2024-11-10 14:00:00', 'Pending');";

             // Execute the insert statements
             session.execute(SimpleStatement.newInstance(insertDataQuery1));
             session.execute(SimpleStatement.newInstance(insertDataQuery2));
             session.execute(SimpleStatement.newInstance(insertDataQuery3));
             session.execute(SimpleStatement.newInstance(insertDataQuery4));
             session.execute(SimpleStatement.newInstance(insertDataQuery5));

             System.out.println("Sample data inserted successfully.");
         } catch (Exception e) {
             e.printStackTrace();
         }
	}
	
	public static void Ejercicio_1(CqlSession session, String nombreProducto, String nombreDeposito) {
		
		// Filter by `nombreDeposito` and `nombreProducto`
		String selectQuery = "SELECT * FROM products WHERE nombreDeposito = ? AND nombreProducto = ?;";
		
		// String selectQuery = "SELECT * FROM products WHERE nombreDeposito = '" + nombreDeposito + "' AND nombreProducto = '" + nombreProducto + "';"; ALTERNATIVA más simple
		
		// Prepare the statement
		SimpleStatement selectStatement = SimpleStatement.builder(selectQuery)
		        .addPositionalValue(nombreDeposito)  // Bind the value for nombreDeposito
		        .addPositionalValue(nombreProducto)  // Bind the value for nombreProducto
		        .build();

		// Execute the query
		ResultSet resultSet = session.execute(selectStatement);

		// Iterate over the results and print them
		for (Row row : resultSet) {
			System.out.println("-----------------------------------------------------");
		    System.out.println("ID del Deposito: " + row.getUuid("idDeposito"));
		    System.out.println("Deposito: " + row.getString("nombreDeposito"));
		    System.out.println("Ubicacion: " + row.getString("Ubicacion"));
			System.out.println("ID del Producto: " + row.getUuid("idProducto"));
		    System.out.println("Producto: " + row.getString("nombreProducto"));
		    System.out.println("Descripcion: " + row.getString("Descripcion"));
		    System.out.println("-----------------------------------------------------");
		}

	}
	
	public static void Ejercicio_1_Datos (CqlSession session) {
		
		// Create Table
		 
		String createTableQuery =
		 "CREATE TABLE IF NOT EXISTS products (idProducto UUID, nombreProducto TEXT, Descripción TEXT, idDeposito UUID, nombreDeposito TEXT, Ubicación TEXT, PRIMARY KEY ((nombreDeposito, nombreProducto), idDeposito, idProducto));"
		 ;
		 SimpleStatement statement = SimpleStatement.builder(createTableQuery).build();
		  try {
			  session.execute(statement);
			  System.out.println("Table created");
			  } catch (Exception e) {
			  System.out.println("Table already exists"); 
			  }
		  
		  // Insert Data
		   String insertDataQuery1 =
		   "INSERT INTO products (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, Ubicacion) VALUES (uuid(), 'Banana Cavendish', 'Bananas', uuid(), 'Deposito Pepe', 'Av.Corrientes 1150');"
		   ;
		   String insertDataQuery2 =
		   "INSERT INTO products (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, Ubicacion) VALUES (uuid(), 'Manzanas Ringo', 'Manzanas', uuid(), 'Deposito Pepe', 'Av.Corrientes 1150');"
		  ;
		   String insertDataQuery3 =
		   "INSERT INTO products (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, Ubicacion) VALUES (uuid(), 'Limónes', 'Limones amarillos', uuid(), 'Deposito Juan', 'Av.Santa Fe 1132');"
		  ;
		  
		  SimpleStatement insertStatement1 =
		  SimpleStatement.builder(insertDataQuery1).build();
		  SimpleStatement insertStatement2 =
		  SimpleStatement.builder(insertDataQuery2).build();
		  SimpleStatement insertStatement3 =
		  SimpleStatement.builder(insertDataQuery3).build();
		   
		   try {
		   session.execute(insertStatement1);
		   session.execute(insertStatement2);
		   session.execute(insertStatement3);
		   System.out.println("Data inserted");
		   } catch (Exception e) {
		   System.out.println("Failed to insert data: " + e.getMessage());
		   }
	}
}








/*
 * // CQL statement to create the keyspace
 * String createKeyspaceQuery = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
 * " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
 * SimpleStatement createKeyspaceStatement =
 * SimpleStatement.builder(createKeyspaceQuery).build();
 * session.execute(createKeyspaceStatement);
 * System.out.println("Keyspace created: " + keyspace);
 */

/*
 * 
 * for (Row row : resultSet) {
 * System.out.println(row.getString("table_name"));
 * }
 */

/*
 * String query =
 * "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '" +
 * keyspace + "';";
 * SimpleStatement statement = SimpleStatement.builder(query).build();
 * ResultSet resultSet = session.execute(statement);
 * 
 */

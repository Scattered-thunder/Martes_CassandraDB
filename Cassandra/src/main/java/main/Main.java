package main;

import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public class Main {
	public static void main(String[] args) throws ParseException {
		
		String keyspace = "harry";
		try (CqlSession session = CqlSession.builder()
				.addContactPoint(new InetSocketAddress("localhost", 9042))
				.withLocalDatacenter("datacenter1")
				.withKeyspace(keyspace)
				.build()) {
			System.out.println("Connected to Cassandra");
			/*
			Ejercicio_1(session, "Banana Cavendish", "Deposito Pepe");
			Ejercicio_2_Datos(session);
			Ejercicio_2(session, "Product A");
			Ejercicio_3_Datos(session);
			Ejercicio_3(session, 2);
			Ejercicio_4_Datos(session);
			Ejercicio_4(session, "Producto 1");
			Ejercicio_5_Datos(session);
			Ejercicio_5(session, 1);
			Ejercicio_6_Datos(session);
			Ejercicio_6(session);
			Ejercicio_7_Datos(session);
			Ejercicio_7(session, "Juan Perez");
			Ejercicio_8_Datos(session);
			Ejercicio_8(session, "Producto A");
			Ejercicio_9_Datos(session);
			Ejercicio_9(session, "Carlos Ruiz", "2024-03-01 00:00:00", "2024-11-06 09:00:00");
   			*/
		}
		
	}
	
	public static void Ejercicio_9(CqlSession session, String nombreCliente, String FechaIni, String FechaFin) throws ParseException {
		
		 	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	        Date fechaIniDate = sdf.parse(FechaIni);
	        Date fechaFinDate = sdf.parse(FechaFin);
	        long fechaIniMillis = fechaIniDate.getTime();
	        long fechaFinMillis = fechaFinDate.getTime();
	        
		System.out.println(nombreCliente);
	        
		String selectQuery = "SELECT SUM(Cantidad * Precio) AS total_value FROM PedidosporClienteFechas " +
                "WHERE nombreCliente = ? AND fechaPedido >= ? AND fechaPedido <= ? " +
                "GROUP BY nombreCliente;";
		
		SimpleStatement simpleStatement = SimpleStatement.builder(selectQuery)
			    .addPositionalValue(nombreCliente)
			    .addPositionalValue(fechaIniMillis)
			    .addPositionalValues(fechaFinMillis)
			    .build();
		
		ResultSet resultSet = session.execute(simpleStatement);
		
		for(Row row: resultSet) {
			System.out.println("-----------------------------------------------------");
			System.out.println("Valor Total: " + row.getInt("total_value"));
			System.out.println("-----------------------------------------------------");
		}
	}
	
	public static void Ejercicio_9_Datos(CqlSession session) {
		
		String createTableQuery = "CREATE TABLE IF NOT EXISTS PedidosporClienteFechas (idCliente INT, nombreCliente TEXT, idPedido INT, idProducto INT, nombreProducto TEXT, fechaPedido TIMESTAMP, estadoEnvio TEXT, Cantidad INT, Precio INT,"
				+ "PRIMARY KEY(nombreCliente, fechaPedido));";
		
		SimpleStatement simpleStatement = SimpleStatement.builder(createTableQuery).build();
		
		try {
			session.execute(simpleStatement);
			System.out.println("Tabla creada exitosamente!");
		} catch(Exception e){
			System.out.println("Error: " + e.getMessage());
		}
		
		try {
            String insert1 = "INSERT INTO PedidosporClienteFechas (idCliente, nombreCliente, idPedido, idProducto, nombreProducto, fechaPedido, estadoEnvio, Cantidad, Precio) VALUES (1, 'Juan Perez', 1001, 201, 'Producto A', '2024-11-01T10:00:00', 'Enviado', 2, 200);";
            String insert2 = "INSERT INTO PedidosporClienteFechas (idCliente, nombreCliente, idPedido, idProducto, nombreProducto, fechaPedido, estadoEnvio, Cantidad, Precio) VALUES (1, 'Juan Perez', 1002, 202, 'Producto B', '2024-11-02T12:30:00', 'Pendiente', 1, 150);";
            String insert3 = "INSERT INTO PedidosporClienteFechas (idCliente, nombreCliente, idPedido, idProducto, nombreProducto, fechaPedido, estadoEnvio, Cantidad, Precio) VALUES (2, 'Maria Lopez', 1003, 203, 'Producto C', '2024-11-03T09:15:00', 'Enviado',5 ,300);";
            String insert4 = "INSERT INTO PedidosporClienteFechas (idCliente,nombreCliente ,idPedido ,idProducto ,nombreProducto ,fechaPedido ,estadoEnvio ,Cantidad ,Precio ) VALUES (2,'Maria Lopez',1004 ,204 ,'Producto D','2024-11-04T14:45:00','Cancelado',3 ,400);";
            String insert5 = "INSERT INTO PedidosporClienteFechas (idCliente,nombreCliente ,idPedido ,idProducto ,nombreProducto ,fechaPedido ,estadoEnvio ,Cantidad ,Precio ) VALUES (3,'Carlos Ruiz',1005 ,205 ,'Producto E','2024-11-05T08:00:00','Enviado',4 ,250);";
            String insert6 = "INSERT INTO PedidosporClienteFechas (idCliente,nombreCliente ,idPedido ,idProducto ,nombreProducto ,fechaPedido ,estadoEnvio ,Cantidad ,Precio ) VALUES (3,'Carlos Ruiz',1006 ,206 ,'Producto F','2024-05-06T11:30:00','Pendiente',2 ,350);";
            String insert7 = "INSERT INTO PedidosporClienteFechas (idCliente,nombreCliente ,idPedido ,idProducto ,nombreProducto ,fechaPedido ,estadoEnvio ,Cantidad ,Precio ) VALUES (4,'Ana Torres',1007 ,207 ,'Producto G','2024-11-07T13:00:00','Enviado' ,1 ,500);";

            session.execute(SimpleStatement.newInstance(insert1));
            session.execute(SimpleStatement.newInstance(insert2));
            session.execute(SimpleStatement.newInstance(insert3));
            session.execute(SimpleStatement.newInstance(insert4));
            session.execute(SimpleStatement.newInstance(insert5));
            session.execute(SimpleStatement.newInstance(insert6));
            session.execute(SimpleStatement.newInstance(insert7));

            System.out.println("Datos insertados con éxito en la tabla PedidosporClienteFechas.");
        } catch(Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
	
	public static void Ejercicio_8(CqlSession session, String nombreProducto) {
		
		String selectQuery = "SELECT * FROM EnviosPorProducto WHERE nombreProducto = ?;";
		
		SimpleStatement simpleStatement = SimpleStatement.builder(selectQuery)
				.addPositionalValues(nombreProducto)
				.build();
		
		ResultSet resultSet = session.execute(simpleStatement);
		
		for(Row row: resultSet) {
			
			System.out.println("-----------------------------------------------------");
			System.out.println("ID Envio: " + row.getInt("idEnvio"));
			System.out.println("Estado del Envio: " + row.getString("estadoenvio"));
			System.out.println("Fecha: " + row.getInstant("fechaenvio"));
			System.out.println("Nombre del Depósitos " + row.getString("depositonombre"));
			System.out.println("-----------------------------------------------------");
			
		}
		
	}
	
	public static void Ejercicio_8_Datos(CqlSession session) {
		
		String createTableQuery = "CREATE TABLE IF NOT EXISTS EnviosPorProducto (idProducto INT, nombreProducto TEXT, depositoNombre TEXT, idDeposito INT, estadoEnvio TEXT, idEnvio INT, fechaEnvio TIMESTAMP, transportista TEXT,"
				+ "PRIMARY KEY(nombreProducto, idEnvio, depositoNombre));";
		
		SimpleStatement simpleStatement = SimpleStatement.builder(createTableQuery).build();
		
		try {
			session.execute(simpleStatement);
			System.out.println("Tabla creada exitosamente");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
		
		 try {
	            String insert1 = "INSERT INTO EnviosPorProducto (idProducto, nombreProducto, depositoNombre, idDeposito, estadoEnvio, idEnvio, fechaEnvio, transportista) VALUES (201, 'Producto A', 'Depósito A', 1, 'Enviado', 3001, '2024-11-01 10:00:00', 'Transportista X');";
	            String insert2 = "INSERT INTO EnviosPorProducto (idProducto, nombreProducto, depositoNombre, idDeposito, estadoEnvio, idEnvio, fechaEnvio, transportista) VALUES (201, 'Producto A', 'Depósito A', 1, 'Pendiente', 3002, '2024-11-02 12:30:00', 'Transportista Y');";
	            String insert3 = "INSERT INTO EnviosPorProducto (idProducto, nombreProducto, depositoNombre, idDeposito, estadoEnvio, idEnvio, fechaEnvio, transportista) VALUES (202,' Producto B','Depósito A', 1,'Enviado', 3003,'2024-11-03 09:15:00','Transportista Z');";
	            String insert4 = "INSERT INTO EnviosPorProducto (idProducto,nombreProducto ,depositoNombre ,idDeposito ,estadoEnvio ,idEnvio ,fechaEnvio ,transportista ) VALUES (203,' Producto C','Depósito B',2,'Enviado',3004,'2024-11-04 14:45:00','Transportista X');";
	            String insert5 = "INSERT INTO EnviosPorProducto (idProducto,nombreProducto ,depositoNombre ,idDeposito ,estadoEnvio ,idEnvio ,fechaEnvio ,transportista ) VALUES (201,' Producto A','Depósito B',2,'Cancelado',3005,'2024-11-05 08:00:00','Transportista Y');";
	            String insert6 = "INSERT INTO EnviosPorProducto (idProducto,nombreProducto ,depositoNombre ,idDeposito ,estadoEnvio ,idEnvio ,fechaEnvio ,transportista ) VALUES (204,' Producto D','Depósito C',3,'Enviado',3006,'2024-11-06 11:30:00','Transportista Z');";
	            String insert7 = "INSERT INTO EnviosPorProducto (idProducto,nombreProducto ,depositoNombre ,idDeposito ,estadoEnvio ,idEnvio ,fechaEnvio ,transportista ) VALUES (202,' Producto B','Depósito C',3,'Pendiente',3007,'2024-11-07 13:00:00','Transportista X');";
	            String insert8 = "INSERT INTO EnviosPorProducto (idProducto,nombreProducto ,depositoNombre ,idDeposito ,estadoEnvio ,idEnvio ,fechaEnvio ,transportista ) VALUES (205,' Producto E','Depósito D',4,'Enviado',3008,'2024-11-08 15:45:00','Transportista Y');";
	            String insert9 = "INSERT INTO EnviosPorProducto (idProducto,nombreProducto ,depositoNombre ,idDeposito ,estadoEnvio ,idEnvio ,fechaEnvio ,transportista ) VALUES (203,' Producto C','Depósito D',4,'Pendiente',3009,'2024-11-09 17:30:00','Transportista Z');";

	            session.execute(SimpleStatement.newInstance(insert1));
	            session.execute(SimpleStatement.newInstance(insert2));
	            session.execute(SimpleStatement.newInstance(insert3));
	            session.execute(SimpleStatement.newInstance(insert4));
	            session.execute(SimpleStatement.newInstance(insert5));
	            session.execute(SimpleStatement.newInstance(insert6));
	            session.execute(SimpleStatement.newInstance(insert7));
	            session.execute(SimpleStatement.newInstance(insert8));
	            session.execute(SimpleStatement.newInstance(insert9));

	            System.out.println("Datos insertados con éxito en la tabla EnviosPorProducto.");
	        } catch(Exception e) {
	            System.out.println("Error: " + e.getMessage());
	        }
	}
	
	public static void Ejercicio_7(CqlSession session, String nombreCliente) {
		
		String selectQuery = "SELECT * FROM PedidosPorCliente WHERE nombreCliente = ?;";
		
		SimpleStatement simpleStatement = SimpleStatement.builder(selectQuery)
				.addPositionalValue(nombreCliente)
				.build();
		
		ResultSet resultSet = session.execute(simpleStatement);
		
		for(Row row: resultSet) {
			System.out.println("-----------------------------------------------------");
			System.out.println("ID del Pedido: " + row.getInt("idpedido"));
			System.out.println("Estado del Pedido: " + row.getString("estadopedido"));
			System.out.println("ID del Producto: " + row.getInt("idproducto"));
			System.out.println("Nombre de Producto: " + row.getString("nombreproducto"));
			System.out.println("Deposito: " + row.getString("deposito"));
			System.out.println("Estado del Envio: " + row.getString("estadoenvio"));
			System.out.println("-----------------------------------------------------");
		}
		
	}
	
	public static void Ejercicio_7_Datos(CqlSession session) {
		
		String createTableQuery = "CREATE TABLE IF NOT EXISTS PedidosPorCliente (idCliente INT, nombreCliente TEXT, estadoEnvio TEXT, idPedido INT, idProducto INT, nombreProducto TEXT, deposito TEXT, estadoPedido TEXT,"
				+ "PRIMARY KEY(nombreCliente, idPedido, nombreProducto));";
		
		SimpleStatement simpleStatement = SimpleStatement.builder(createTableQuery).build();
		
		try {
			session.execute(simpleStatement);
			System.out.println("Tabla creada!");
		} catch(Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
		
		 try {
		    String insert1 = "INSERT INTO PedidosPorCliente (idCliente, nombreCliente, estadoEnvio, idPedido, idProducto, nombreProducto, deposito, estadoPedido) VALUES (1, 'Juan Perez', 'Enviado', 1001, 201, 'Producto A', 'Depósito A', 'Completo');";
	            String insert2 = "INSERT INTO PedidosPorCliente (idCliente, nombreCliente, estadoEnvio, idPedido, idProducto, nombreProducto, deposito, estadoPedido) VALUES (1, 'Juan Perez', 'Enviado', 1002, 202, 'Producto B', 'Depósito A', 'Completo');";
	            String insert3 = "INSERT INTO PedidosPorCliente (idCliente, nombreCliente, estadoEnvio, idPedido, idProducto, nombreProducto, deposito, estadoPedido) VALUES (2, 'Maria Lopez', 'Pendiente', 1003, 203, 'Producto C', 'Depósito B', 'Pendiente');";
	            String insert4 = "INSERT INTO PedidosPorCliente (idCliente, nombreCliente, estadoEnvio, idPedido, idProducto, nombreProducto, deposito, estadoPedido) VALUES (2, 'Maria Lopez', 'Enviado', 1004, 204, 'Producto D', 'Depósito B', 'Completo');";
	            String insert5 = "INSERT INTO PedidosPorCliente (idCliente, nombreCliente, estadoEnvio, idPedido, idProducto, nombreProducto, deposito, estadoPedido) VALUES (3, 'Carlos Ruiz', 'Cancelado', 1005, 205, 'Producto E', 'Depósito C', 'Cancelado');";
	            String insert6 = "INSERT INTO PedidosPorCliente (idCliente, nombreCliente, estadoEnvio, idPedido, idProducto, nombreProducto, deposito, estadoPedido) VALUES (3,'Carlos Ruiz','Enviado',1006 ,206 ,'Producto F','Depósito C','Completo');";
	            String insert7 = "INSERT INTO PedidosPorCliente (idCliente,nombreCliente ,estadoEnvio,idPedido,idProducto,nombreProducto,deposito ,estadoPedido ) VALUES (4,'Ana Torres','Pendiente' ,1007 ,207 ,'Producto G','Depósito D','Pendiente');";

	            session.execute(SimpleStatement.newInstance(insert1));
	            session.execute(SimpleStatement.newInstance(insert2));
	            session.execute(SimpleStatement.newInstance(insert3));
	            session.execute(SimpleStatement.newInstance(insert4));
	            session.execute(SimpleStatement.newInstance(insert5));
	            session.execute(SimpleStatement.newInstance(insert6));
	            session.execute(SimpleStatement.newInstance(insert7));

	            System.out.println("Datos insertados con éxito en la tabla PedidosPorCliente.");
	        } catch(Exception e) {
	            System.out.println("Error: " + e.getMessage());
	        }
	    }
	
	public static void Ejercicio_6(CqlSession session) {
		
		String selectQuery = "SELECT nombreDeposito, COUNT(idProducto) AS total_productos, SUM(stock * precio) AS valor_total FROM TotalDepositos GROUP BY nombreDeposito";
		
		ResultSet resultSet = session.execute(selectQuery);
		
		for(Row row: resultSet) {
			System.out.println("-----------------------------------------------------");
			System.out.println("Nombre del deposito: " + row.getString("nombreDeposito"));
			System.out.println("Cantidad de productos totales: " + row.getLong("total_productos"));
			System.out.println("Valor total: " + row.getInt("valor_total"));
			System.out.println("-----------------------------------------------------");
		}
		
	}
	
	public static void Ejercicio_6_Datos(CqlSession session) {
		
		String createTableQuery = "CREATE TABLE IF NOT EXISTS TotalDepositos (idProducto INT, nombreProducto TEXT,"
				+ " Descripcion TEXT, idDeposito INT, nombreDeposito TEXT, ubicacion TEXT, stock INT, Precio INT, PRIMARY KEY(nombreDeposito, nombreProducto, idDeposito, idProducto))";
	
		SimpleStatement simpleStatement = SimpleStatement.builder(createTableQuery).build();
		
		try {
			session.execute(simpleStatement);
			System.out.println("Tabla creada con éxito");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
		
	     try {
	            String insert1 = "INSERT INTO TotalDepositos (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, ubicacion, stock, Precio) VALUES (101, 'Producto A', 'Descripción A', 1, 'Depósito A', 'Ubicación A', 50, 200);";
	            String insert2 = "INSERT INTO TotalDepositos (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, ubicacion, stock, Precio) VALUES (102, 'Producto B', 'Descripción B', 1, 'Depósito A', 'Ubicación A', 30, 150);";
	            String insert3 = "INSERT INTO TotalDepositos (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, ubicacion, stock, Precio) VALUES (103, 'Producto C', 'Descripción C', 2, 'Depósito B', 'Ubicación B', 20, 300);";
	            String insert4 = "INSERT INTO TotalDepositos (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, ubicacion, stock, Precio) VALUES (104, 'Producto D', 'Descripción D', 2, 'Depósito B', 'Ubicación B', 10, 400);";
	            String insert5 = "INSERT INTO TotalDepositos (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, ubicacion, stock, Precio) VALUES (105, 'Producto E', 'Descripción E', 3, 'Depósito C', 'Ubicación C', 25, 250);";
	            String insert6 = "INSERT INTO TotalDepositos (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, ubicacion, stock, Precio) VALUES (106, 'Producto F', 'Descripción F', 3, 'Depósito C', 'Ubicación C', 15, 350);";
	            String insert7 = "INSERT INTO TotalDepositos (idProducto, nombreProducto, Descripcion, idDeposito, nombreDeposito, ubicacion, stock, Precio) VALUES (107,' Producto G','Descripción G',4,'Depósito D','Ubicación D',5 ,500);";

	            session.execute(SimpleStatement.newInstance(insert1));
	            session.execute(SimpleStatement.newInstance(insert2));
	            session.execute(SimpleStatement.newInstance(insert3));
	            session.execute(SimpleStatement.newInstance(insert4));
	            session.execute(SimpleStatement.newInstance(insert5));
	            session.execute(SimpleStatement.newInstance(insert6));
	            session.execute(SimpleStatement.newInstance(insert7));

	            System.out.println("Datos insertados con éxito en la tabla TotalDepositos.");
	        } catch (Exception e) {
	            System.out.println("Error: " + e.getMessage());
	        }
	}
	
	
	public static void Ejercicio_5(CqlSession session, Integer idEnvio) {
		
		String createSelectQuery = "SELECT * FROM ProductosporEnvio WHERE idEnvio = ?;";
		
		SimpleStatement simpleStatement = SimpleStatement.builder(createSelectQuery)
				.addPositionalValue(idEnvio)
				.build();
		
		ResultSet resultSet = session.execute(simpleStatement);
		
		for(Row row: resultSet) {
			System.out.println("-----------------------------------------------------");
			System.out.println("ID del Envio: " + row.getInt("idEnvio"));
			System.out.println("ID del Producto: " + row.getInt("idProducto"));
		    System.out.println("Nombre del Producto: " + row.getString("nombreProducto"));
		    System.out.println("-----------------------------------------------------");
		}
		
	}
	
	public static void Ejercicio_5_Datos(CqlSession session) {
		
		String createTableQuery = "CREATE TABLE IF NOT EXISTS ProductosporEnvio (" +
		"idEnvio INT, " + 
		"idProducto INT, " +
		"nombreProducto TEXT, " +
		"PRIMARY KEY(idEnvio, nombreProducto)" +
		");";
				
		SimpleStatement simpleStatement = SimpleStatement.builder(createTableQuery).build();
		
		try {
			  session.execute(simpleStatement);
			  System.out.println("Table created");
		} catch (Exception e) {
			  System.out.println("Table already exists" + e.getMessage()); 
		}
		
		try {
			
		    String insert1 = "INSERT INTO ProductosporEnvio (idEnvio, idProducto, nombreProducto) VALUES (1, 101, 'Producto A');";
		    String insert2 = "INSERT INTO ProductosporEnvio (idEnvio, idProducto, nombreProducto) VALUES (1, 102, 'Producto B');";
		    String insert3 = "INSERT INTO ProductosporEnvio (idEnvio, idProducto, nombreProducto) VALUES (2, 103, 'Producto C');";
		    String insert4 = "INSERT INTO ProductosporEnvio (idEnvio, idProducto, nombreProducto) VALUES (2, 101, 'Producto A');";
		    String insert5 = "INSERT INTO ProductosporEnvio (idEnvio, idProducto, nombreProducto) VALUES (3, 104, 'Producto D');";
		    String insert6 = "INSERT INTO ProductosporEnvio (idEnvio, idProducto, nombreProducto) VALUES (3, 105, 'Producto E');";
		    String insert7 = "INSERT INTO ProductosporEnvio (idEnvio, idProducto, nombreProducto) VALUES (4, 106, 'Producto F');";

		    session.execute(SimpleStatement.newInstance(insert1));
		    session.execute(SimpleStatement.newInstance(insert2));
		    session.execute(SimpleStatement.newInstance(insert3));
		    session.execute(SimpleStatement.newInstance(insert4));
		    session.execute(SimpleStatement.newInstance(insert5));
		    session.execute(SimpleStatement.newInstance(insert6));
		    session.execute(SimpleStatement.newInstance(insert7));		    
		    }
		catch (Exception e) {
			   System.out.println("Failed to insert data: " + e.getMessage());
		}
	}
	
	public static void Ejercicio_4(CqlSession session, String nombreProducto) {
		
		String selectQuery = "SELECT * FROM DepositosporProducto WHERE nombreProducto = ?;";
		
		SimpleStatement simpleStatement = SimpleStatement.builder(selectQuery)
				.addPositionalValue(nombreProducto)
				.build();
		
		ResultSet resultSet = session.execute(simpleStatement);
		
		for(Row row: resultSet) {
			System.out.println("-----------------------------------------------------");
			System.out.println("ID del Deposito: " + row.getInt("idDeposito"));
			System.out.println("ID del Producto: " + row.getInt("idProducto"));
		    System.out.println("Nombre del Producto: " + row.getString("nombreProducto"));
		    System.out.println("Nombre del Deposito: " + row.getString("nombreDeposito"));
		    System.out.println("-----------------------------------------------------");
		}
	}
	
	public static void Ejercicio_4_Datos(CqlSession session) {
		
		String createTableQuery = "CREATE TABLE IF NOT EXISTS DepositosporProducto (" + 
				"nombreDeposito TEXT, " +
				"nombreProducto TEXT, " +
				"idProducto INT, " +
				"idDeposito INT, " +
				"PRIMARY KEY (nombreProducto, nombreDeposito)" +
				");";
				
		SimpleStatement simpleStatement = SimpleStatement.builder(createTableQuery).build();
		
        try {
			  session.execute(simpleStatement);
			  System.out.println("Table created");
			  } catch (Exception e) {
			  System.out.println("Table already exists"); 
			  }
        try {
            String insert1 = "INSERT INTO DepositosporProducto (nombreDeposito, nombreProducto, idProducto, idDeposito) VALUES ('Depósito A', 'Producto 1', 101, 1);";
            String insert2 = "INSERT INTO DepositosporProducto (nombreDeposito, nombreProducto, idProducto, idDeposito) VALUES ('Depósito A', 'Producto 2', 102, 2);";
            String insert3 = "INSERT INTO DepositosporProducto (nombreDeposito, nombreProducto, idProducto, idDeposito) VALUES ('Depósito B', 'Producto 1', 101, 3);";
            String insert4 = "INSERT INTO DepositosporProducto (nombreDeposito, nombreProducto, idProducto, idDeposito) VALUES ('Depósito B', 'Producto 3', 103, 4);";
            String insert5 = "INSERT INTO DepositosporProducto (nombreDeposito, nombreProducto, idProducto, idDeposito) VALUES ('Depósito C', 'Producto 2', 102, 5);";
            String insert6 = "INSERT INTO DepositosporProducto (nombreDeposito, nombreProducto, idProducto, idDeposito) VALUES ('Depósito C', 'Producto 3', 103, 6);";
            String insert7 = "INSERT INTO DepositosporProducto (nombreDeposito, nombreProducto, idProducto, idDeposito) VALUES ('Depósito D', 'Producto 1', 101, 7);";
            String insert8 = "INSERT INTO DepositosporProducto (nombreDeposito, nombreProducto, idProducto, idDeposito) VALUES ('Depósito D', 'Producto 4', 104, 8);";

            session.execute(SimpleStatement.newInstance(insert1));
            session.execute(SimpleStatement.newInstance(insert2));
            session.execute(SimpleStatement.newInstance(insert3));
            session.execute(SimpleStatement.newInstance(insert4));
            session.execute(SimpleStatement.newInstance(insert5));
            session.execute(SimpleStatement.newInstance(insert6));
            session.execute(SimpleStatement.newInstance(insert7));
            session.execute(SimpleStatement.newInstance(insert8));
		
	} catch (Exception e) {
		   System.out.println("Failed to insert data: " + e.getMessage());
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
		
		String selectQuery = "SELECT * FROM products WHERE nombreDeposito = ? AND nombreProducto = ?;";
		
		SimpleStatement selectStatement = SimpleStatement.builder(selectQuery)
		        .addPositionalValue(nombreDeposito)  
		        .addPositionalValue(nombreProducto)  
		        .build();

		ResultSet resultSet = session.execute(selectStatement);

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
  CQL statement to create the keyspace
  String createKeyspaceQuery = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
  " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
  SimpleStatement createKeyspaceStatement =
  SimpleStatement.builder(createKeyspaceQuery).build();
  session.execute(createKeyspaceStatement);
  System.out.println("Keyspace created: " + keyspace);
 */

/*
 
  for (Row row : resultSet) {
  System.out.println(row.getString("table_name"));
  }
  
 */

/*
  String query =
  "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '" +
  keyspace + "';";
  SimpleStatement statement = SimpleStatement.builder(query).build();
  ResultSet resultSet = session.execute(statement);
 
 */

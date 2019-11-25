package edu.osu.cse5234.batch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class InventoryUpdater {

	public static void main(String[] args) {
		System.out.println("Starting Inventory Update ...");
		try {
			Connection conn = createConnection();
			Collection<Integer> newOrderIds = getNewOrders(conn);
			Map<Integer, Integer> orderedItems = getOrderedLineItems(newOrderIds, conn);
			udpateInventory(orderedItems, conn);
			udpateOrderStatus(newOrderIds, conn);
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Connection createConnection() throws SQLException, ClassNotFoundException {
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.getConnection("jdbc:h2:D:\\h2db\\GumShopDB\\GumShopDB;AUTO_SERVER=TRUE", "sa", "");
		return conn;
	}

	private static Collection<Integer> getNewOrders(Connection conn) throws SQLException {
		Collection<Integer> orderIds = new ArrayList<Integer>();
		ResultSet rset = conn.createStatement().executeQuery("select ID from CUSTOMER_ORDER where STATUS = 'New'");
		while (rset.next()) {
			orderIds.add(new Integer(rset.getInt("ID")));
		}
		return orderIds;
	}

	private static Map<Integer, Integer> getOrderedLineItems(Collection<Integer> newOrderIds, Connection conn) throws SQLException {
		// TODO Auto-generated method stub
		// This method returns a map of two integers. The first Integer is item ID, and 
        // the second is cumulative requested quantity across all new orders
		Map<Integer, Integer> res = new HashMap<>();
		for (Integer id : newOrderIds) {
			ResultSet rset = conn.createStatement().executeQuery("select ITEM_NUMBER, QUANTITY from CUSTOMER_ORDER_LINE_ITEM where CUSTOMER_ORDER_ID_FK = " + id);
			while (rset.next()) {
				Integer itemId = new Integer(rset.getInt("ITEM_NUMBER"));
				if (!res.containsKey(itemId)) {
					res.put(itemId, new Integer(0));
				}
				res.put(itemId, res.get(itemId) + new Integer(rset.getInt("QUANTITY")));
			}
		}
		return res;
	}

	private static void udpateInventory(Map<Integer, Integer> orderedItems, Connection conn) throws SQLException {
		// TODO Auto-generated method stub
		for (Map.Entry<Integer, Integer> entry : orderedItems.entrySet()) {
			ResultSet rset = conn.createStatement().executeQuery("select AVAILABLE_QUANTITY from ITEM where ITEM_NUMBER = " + entry.getKey());
			while (rset.next()) {
				int quantity = rset.getInt("AVAILABLE_QUANTITY");
				conn.createStatement().executeUpdate("update ITEM set AVAILABLE_QUANTITY = " + (quantity - entry.getValue()) + " where ITEM_NUMBER = " + entry.getKey());
			}
		}
	}

	private static void udpateOrderStatus(Collection<Integer> newOrderIds, Connection conn) throws SQLException {
		// TODO Auto-generated method stub
		for (Integer id : newOrderIds) {
			conn.createStatement().executeUpdate("update CUSTOMER_ORDER set STATUS = 'Finished' where ID = " + id);
		}
	}
}

package programmingAssignment2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Scanner;

public class DavisBaseMain {
	static Scanner sc = new Scanner(System.in).useDelimiter(";");
	public static String workingDir = System.getProperty("user.dir");
	public static String catalogDir = workingDir + "\\data\\catalog";
	public static String databaseTablesTable = workingDir + "\\data\\catalog\\davisbase_tables.tbl";
	public static String databaseColumnsTable = workingDir + "\\data\\catalog\\davisbase_columns.tbl";
	static boolean isExit = false;
	static String currDatabase = "";
	static long pageSize = 512;

	public static void main(String[] args) {
		try {
			createDirs();
		} catch (IOException e) {
			e.printStackTrace();
		}
		help();
		String userCommand = "";
		while (!isExit) {
			System.out.println("davisql>");
			userCommand = sc.next().replace("\n", " ").replace("\r", " ").trim().toLowerCase();
			userCommand = userCommand.replaceAll(",", " , ");
			userCommand = userCommand.replaceAll(" +", " ");
			userCommand = userCommand.replace("( ", "(");
			userCommand = userCommand.replace(" )", ")");
			parseUserCommand(userCommand);
		}
	}

	private static void createDirs() throws IOException {
		File f = new File(workingDir + "\\data\\catalog");
		if (!f.exists()) {
			f.mkdirs();
			RandomAccessFile tableFile = new RandomAccessFile(workingDir + "\\data\\catalog\\davisbase_tables.tbl",
					"rw");
			tableFile.setLength(pageSize);
			LeafPage dBTblTbl = new LeafPage();
			LinkedHashMap<String, Integer> dbTbLHM = new LinkedHashMap<String, Integer>();
			dbTbLHM.put("davisbase_tables", 30);
			dbTbLHM.put("information_schema", 20);
			dBTblTbl.leafPageHeader(tableFile);
			dBTblTbl.checkNumOfRecords(tableFile);
			dBTblTbl.LeafPageRecord(tableFile, dbTbLHM, 59);
			tableFile.close();
			tableFile = new RandomAccessFile(workingDir + "\\data\\catalog\\davisbase_columns.tbl", "rw");

			tableFile.setLength(pageSize);

			ArrayList colNameLs = new ArrayList<String>(Arrays.asList("table_name", "db_name", "col_name", "col_type",
					"col_size", "ordinal_pos", "is_nullable"));
			ArrayList colTypeLs = new ArrayList<String>(
					Arrays.asList("text", "text", "text", "text", "int", "text", "text"));
			ArrayList colSizeLs = new ArrayList(Arrays.asList(20, 20, 20, 10, 4, 5, 1));
			LeafPage dBColTbl1 = new LeafPage();
			dBColTbl1.leafPageHeader(tableFile);
			for (int i = 0; i < 7; i++) {
				LinkedHashMap dbColLHM1 = new LinkedHashMap();
				dbColLHM1.put("davisbase_columns", 20);// table name
				dbColLHM1.put("information_schema", 20);// db name
				dbColLHM1.put(colNameLs.get(i), 20);// col name
				dbColLHM1.put(colTypeLs.get(i), 10);// col type
				dbColLHM1.put(colSizeLs.get(i), 4);// col size
				dbColLHM1.put(i + 1 + "", 5);// ordinal pos
				dbColLHM1.put("N", 1);// is nullable
				// LeafPage dBColTbl1 = new LeafPage();
				dBColTbl1.checkNumOfRecords(tableFile);
				dBColTbl1.LeafPageRecord(tableFile, dbColLHM1, 94);
			}
			
			ArrayList colNameLs1 = new ArrayList<String>(Arrays.asList("table_name", "db_name"));
			ArrayList colTypeLs1 = new ArrayList<String>(
					Arrays.asList("text", "text"));
			ArrayList colSizeLs1 = new ArrayList(Arrays.asList(30, 20));
			
			for (int i = 0; i < 2; i++) {
				LinkedHashMap dbColLHM1 = new LinkedHashMap();
				dbColLHM1.put("davisbase_tables", 20);// table name
				dbColLHM1.put("information_schema", 20);// db name
				dbColLHM1.put(colNameLs1.get(i), 20);// col name
				dbColLHM1.put(colTypeLs1.get(i), 10);// col type
				dbColLHM1.put(colSizeLs1.get(i), 4);// col size
				dbColLHM1.put(i + 1 + "", 5);// ordinal pos
				dbColLHM1.put("N", 1);// is nullable
				// LeafPage dBColTbl1 = new LeafPage();
				dBColTbl1.checkNumOfRecords(tableFile);
				dBColTbl1.LeafPageRecord(tableFile, dbColLHM1, 94);
			}

			tableFile.close();

		}

	}

	public static void parseUserCommand(String userCommand) {
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		switch (commandTokens.get(0)) {
		case "select":
			try {
				parseQuery(userCommand);
			} catch (DataTypeNotFoundExp e1) {
				System.out.println(e1.getMessage());
			}
			break;
		case "drop":
			switch (commandTokens.get(1)) {
			case "table":
				parseDropTable(userCommand);
				break;
			case "database":
				parseDropDatabase(userCommand, commandTokens.get(2));
				break;
			}
			break;
		case "create":
			switch (commandTokens.get(1)) {
			case "table":
				try {
					parseCreateTable(userCommand);
				} catch (DataTypeNotFoundExp e) {
					System.out.println(e.getMessage());
				}
				break;
			case "database":
				parseCreateDatabase(userCommand, commandTokens.get(2));
				break;
			}

			break;
		case "exit":
			isExit = true;
			break;
		case "use":
			parseUseDatabase(userCommand, commandTokens.get(1));
			break;
		case "show":
			switch (commandTokens.get(1)) {
			case "databases":
				parseShowDatabases();
				break;
			case "tables":
				parseShowTables();
				break;
			}
			break;
		case "insert":
			try {
				parseInsertInto(userCommand);
			} catch (DataTypeNotFoundExp e) {
				System.out.println(e.getMessage());
			}
			break;
		case "help":
			help();
			break;
		default:
			System.out.println("Error in parsing : \"" + userCommand + "\"");
			break;
		}
	}

	private static void help() {
		System.out.println("Commands supported :");
		System.out.println("1. Create database\nEg. create database animals;");
		System.out.println("2. Use database\nEg. use animals;");
		System.out.println("3. Create table\nEg.create table cat (id int primary key, name varchar(20), breed varchar(10) );");
		System.out.println("4. Insert Into\nEg.insert into cat ( id ,  name ,  breed ) values (1 ,  snowbell , 12);\nNOTE: datatypes supported int, varchar(any length between 1 and 20) and insert should be in order of columns defined");
		System.out.println("5. select * from table_name & select <column_names separated by commas> from table_name;\nNOTE: select columns should be in order of columns defined");
		System.out.println("6. Drop database. Note:drops database only from file structure");
		System.out.println("7. Drop table. Note:drops table only from file structure");
		System.out.println("8. Show tables;");
		System.out.println("9. Show databases;");
		System.out.println("10. help;");
	}

	private static void parseShowTables() {
		File f = new File(workingDir + "\\data");
		String listOfFolders[] = f.list();
		System.out.println("tables");
		System.out.println("-------");
		for (String folderNames : listOfFolders) {
			if (!folderNames.equals("catalog")) {
				String listOfFiles[] = new File(workingDir + "\\data\\" + folderNames).list();
				for (String fileName : listOfFiles) {
					System.out.println(fileName.substring(0, fileName.length() - 4));
				}
			}
		}
	}

	private static void parseDropDatabase(String userCommand, String dbName) {
		String dropDBName = workingDir + "\\data\\" + dbName;
		File file = new File(dropDBName);
		File[] listOfFile = file.listFiles();
		for (File tempFile : listOfFile) {
			tempFile.delete();
		}
		file.delete();
	}

	private static void parseInsertInto(String userCommand) throws DataTypeNotFoundExp {
		if (currDatabase.equals("")) {
			System.out.println("no database selected!");
		} else {
			String userCommandtmp = userCommand;
			userCommandtmp = userCommand.substring(0, userCommand.indexOf("("));
			ArrayList<String> insertIntoTokens = new ArrayList<String>(Arrays.asList(userCommandtmp.split(" ")));
			userCommandtmp = userCommand.substring(userCommand.indexOf("(") + 1, userCommand.indexOf(")") );
			ArrayList<String> insertIntoColNames = new ArrayList<String>(Arrays.asList(userCommandtmp.split(" , ")));
			userCommandtmp = userCommand.substring(userCommand.lastIndexOf("(") + 1, userCommand.lastIndexOf(")"));
			ArrayList<String> insertIntoValues = new ArrayList<String>(Arrays.asList(userCommandtmp.split(" , ")));
			
			String tableName = insertIntoTokens.get(2);
			LinkedHashMap insertValues = new LinkedHashMap();
			
			ArrayList colsFromDBTbl = new ArrayList();
			LeafPage lp = new LeafPage();
			RandomAccessFile insTbl;
			try {
				insTbl = new RandomAccessFile(databaseColumnsTable, "r");
				colsFromDBTbl = lp.processSelect(insTbl, tableName);
				LinkedHashMap hs = new LinkedHashMap();
				for (int i = colsFromDBTbl.size() - 1, j = 0; i >= 0; i--, j++) {
					ArrayList l = (ArrayList) colsFromDBTbl.get(i);
					if (insertIntoColNames.contains(l.get(2))) {
						hs.put(l.get(2), l.get(3));
					}

				}				
				ArrayList tempColNamesToken = new ArrayList();
				tempColNamesToken.addAll(insertIntoColNames);
				insertIntoColNames.removeAll(hs.keySet());
				
					if (!insertIntoColNames.isEmpty()) {
						throw new DataTypeNotFoundExp("Column type " + insertIntoColNames + " not recognised");
					}
				
				
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}
			
			LinkedHashMap insIntoDet = new LinkedHashMap();
			int colSize = 0;
			int cellSize = 0;
			for (int i = 0; i < insertIntoValues.size(); i++) {
				try {
					int valueToBePut = Integer.parseInt(insertIntoValues.get(i));
					insIntoDet.put(Integer.parseInt(insertIntoValues.get(i)), 4);
					cellSize += 5;
				} catch (NumberFormatException e) {
					insIntoDet.put(insertIntoValues.get(i), 20);
					cellSize += 21;
				}
			}
			
			
			LeafPage insData = new LeafPage();
			
			try {
				RandomAccessFile insTable = new RandomAccessFile(currDatabase + "\\" + tableName + ".tbl", "rw");
				insData.checkNumOfRecords(insTable);
				insData.LeafPageRecord(insTable, insIntoDet, cellSize + 7);
				try {
					insTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			System.out.println("Row inserted!");
		}
	}

	private static void parseShowDatabases() {
		File f = new File(workingDir + "\\data");
		String listOfFiles[] = f.list();
		System.out.println("database_names");
		System.out.println("--------------");
		for (String fileName : listOfFiles) {
			if (!fileName.equals("catalog")) {
				System.out.println(fileName);
			}
		}
	}

	private static void parseUseDatabase(String userCommand, String dbName) {
		File dbNameF = new File(workingDir + "\\data\\" + dbName);
		if (dbNameF.exists()) {
			currDatabase = workingDir + "\\data\\" + dbName;
		} else {
			System.out.println("Database " + dbName + " doesn't exist!");
		}

	}

	private static void parseCreateDatabase(String userCommand, String dataBaseName) {
		File f = new File(workingDir + "\\data\\" + dataBaseName);
		if (!f.exists()) {
			f.mkdirs();
			System.out.println("Database " + dataBaseName + " created");
		} else {
			System.out.println("Database Already Exists!");
		}

	}

	private static void parseCreateTable(String createTableString) throws DataTypeNotFoundExp {
		if (currDatabase.equals("")) {
			System.out.println("no database selected!");
		} else {
			String createTableStringTmp = createTableString;
			createTableStringTmp = createTableStringTmp.substring(0, createTableString.indexOf("(") - 1);
			ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableStringTmp.split(" ")));
			createTableStringTmp = createTableString.substring(createTableString.indexOf("(") + 1,
					createTableString.length() - 1);
			ArrayList<String> createTableTokensVal = new ArrayList<String>(
					Arrays.asList(createTableStringTmp.split(" , ")));
			for (String eachVal : createTableTokensVal) {
				ArrayList<String> eachRow = new ArrayList<String>(Arrays.asList(eachVal.split(" ")));
				for (String eachValInner : eachRow) {
					ArrayList<String> innerVal = new ArrayList<String>(Arrays.asList(eachVal.split(" ")));
					if (!(innerVal.get(1).contains("varchar") || innerVal.get(1).contains("int")
							|| innerVal.get(1).contains("smallint") || innerVal.get(1).contains("tinyint")
							|| innerVal.get(1).contains("double"))) {
						throw new DataTypeNotFoundExp("Data Type " + innerVal.get(1) + " is not recognised!");
					}
				}
			}
			String dBName = currDatabase.substring(currDatabase.lastIndexOf("\\") + 1, currDatabase.length());
			String tableFileName = createTableTokens.get(2) + ".tbl";
			if (new File(currDatabase + "\\" + tableFileName).exists()) {
				System.out.println("Table already exists!");
			} else {

				/* Code to create a .tbl file to contain table data */
				try {
					RandomAccessFile tableFile = new RandomAccessFile(currDatabase + "\\" + tableFileName, "rwd");
					LeafPage insData = new LeafPage();
					insData.leafPageHeader(tableFile);
					tableFile.setLength(pageSize);
					System.out.println("Table created");
					tableFile.close();
				} catch (Exception e) {
					System.out.println(e);
				}

				/*
				 * Code to insert a row in the davisbase_tables table i.e.
				 * database catalog meta-data
				 */
				RandomAccessFile tableInsFile;
				try {
					tableInsFile = new RandomAccessFile(catalogDir + "\\davisbase_tables.tbl", "rw");
					LeafPage lPDBTbl = new LeafPage();
					lPDBTbl.checkNumOfRecords(tableInsFile);
					LinkedHashMap<String, Integer> colDet = new LinkedHashMap<String, Integer>();
					colDet.put(createTableTokens.get(2), 30);
					colDet.put(dBName, 20);
					lPDBTbl.LeafPageRecord(tableInsFile, colDet, 59);
					try {
						tableInsFile.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
				}

				/*
				 * Code to insert rows in the davisbase_columns table for each
				 * column in the new table i.e. database catalog meta-data
				 */
				RandomAccessFile colInsFile;
				try {
					colInsFile = new RandomAccessFile(catalogDir + "\\davisbase_columns.tbl", "rw");

					int ordinalPos = 1;
					for (String eachVal : createTableTokensVal) {
						LeafPage lPDBTbl = new LeafPage();
						lPDBTbl.checkNumOfRecords(colInsFile);
						LinkedHashMap colDet = new LinkedHashMap();
						ArrayList<String> eachRow = new ArrayList<String>(Arrays.asList(eachVal.split(" ")));
						colDet.put(createTableTokens.get(2), 20);// tablename
						colDet.put(dBName, 20);
						colDet.put(eachRow.get(0), 20);// colname
						if (eachRow.get(1).contains("varchar")) {
							colDet.put("text", 10);// datatype
							colDet.put(Integer.parseInt(eachRow.get(1).substring(8, eachRow.get(1).length() - 1)), 4);// size
						} else if (eachRow.get(1).contains("int")) {
							colDet.put("int", 10);
							colDet.put(4, 4);
						} else if (eachRow.get(1).contains("tinyint")) {
							colDet.put("tinyint", 10);
							colDet.put(1, 1);
						} else if (eachRow.get(1).contains("smallint")) {
							colDet.put("smallint", 10);
							colDet.put(2, 2);
						} else if (eachRow.get(1).contains("double")) {
							colDet.put("double", 10);
							colDet.put(8, 8);
						}
						colDet.put(ordinalPos + "", 5);// ordinal position
						if (eachRow.contains("primary") && eachRow.contains("key")) {
							colDet.put("N", 1);// isNullable
						} else {
							colDet.put("Y", 1);// isNullable
						}

						ordinalPos++;

						lPDBTbl.LeafPageRecord(colInsFile, colDet, 94);
					}
					try {
						colInsFile.close();
					} catch (IOException e) {
						e.printStackTrace();
					}

				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
				}
			}
		}

	}

	private static void parseDropTable(String userCommand) {
		if (currDatabase.equals("")) {
			System.out.println("no database selected!");
		} else {
			ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

			String tableFileName = createTableTokens.get(2) + ".tbl";
			File delFile = new File(currDatabase + "\\" + tableFileName);
			if (delFile.exists()) {
				if (delFile.delete()) {
					System.out.println("Table dropped");
				} else {
					System.out.println("Table cannot be dropped");
				}
			} else {
				System.out.println("Table doesn't exist");
			}
		}

	}

	private static void parseQuery(String userCommand) throws DataTypeNotFoundExp {
		if (currDatabase.equals("")) {
			System.out.println("No database selected!");
		} else {
			
			ArrayList<String> selectTableTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
			String tableName;
			tableName = selectTableTokens.get(selectTableTokens.indexOf("from") + 1);
			String colNamesStr = userCommand.substring(userCommand.indexOf("select") + 7,
					userCommand.indexOf("from") - 1);
			ArrayList<String> colNamesTokens = new ArrayList<String>(Arrays.asList(colNamesStr.split(" , ")));

			if (selectTableTokens.contains("where")) {
				String whereClause = userCommand.substring(userCommand.indexOf("where"), userCommand.length());
				ArrayList<String> whereClauseTokens = new ArrayList<String>(Arrays.asList(whereClause.split(" ")));
				for (int i = 0; i < whereClauseTokens.size(); i += 4) {
					String colName = whereClauseTokens.get(i + 1);
					String operator = whereClauseTokens.get(i + 2);
					String valComp = whereClauseTokens.get(i + 3);
				}
			}
			String dBName = currDatabase.substring(currDatabase.lastIndexOf("\\") + 1, currDatabase.length());
			RandomAccessFile tableFile;
			RandomAccessFile dbColsTable;
			try {
				tableFile = new RandomAccessFile(currDatabase + "\\" + tableName + ".tbl", "r");
				dbColsTable = new RandomAccessFile(databaseColumnsTable, "r");
				LeafPage selectQueryProcess = new LeafPage();
				ArrayList queryResults = new ArrayList();
				queryResults = selectQueryProcess.processSelect(dbColsTable, tableName);
				LinkedHashMap hs = new LinkedHashMap();
				for (int i = queryResults.size() - 1, j = 0; i >= 0; i--, j++) {
					ArrayList l = (ArrayList) queryResults.get(i);
					if (colNamesTokens.contains(l.get(2)) || colNamesTokens.contains("*")) {
						hs.put(l.get(2), j);
					}

				}
				ArrayList tempColNamesToken = new ArrayList();
				tempColNamesToken.addAll(colNamesTokens);
				colNamesTokens.removeAll(hs.keySet());
				if (!tempColNamesToken.contains("*")) {
					if (!colNamesTokens.isEmpty()) {
						throw new DataTypeNotFoundExp("Column type " + colNamesTokens + " not recognised");
					}
				}
				queryResults = selectQueryProcess.processSelect(tableFile, "");
				if (tempColNamesToken.contains("*")) {
					tempColNamesToken.addAll(hs.keySet());
					tempColNamesToken.remove("*");
					printSelect(hs, queryResults, tempColNamesToken);
				} else {
					printSelect(hs, queryResults, tempColNamesToken);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

		}

	}

	private static void printSelect(LinkedHashMap hs, ArrayList queryResults, ArrayList<String> colNamesTokens) {
		int space = 0;
		for (int i = 0; i < colNamesTokens.size(); i++) {
			space+=20;
			System.out.print(String.format("%-20s",colNamesTokens.get(i)));
		}
		System.out.print("\n"+String.format(String.format("%%%ds", space), " ").replace(" ","-")+"\n");
		for (int i = queryResults.size() - 1; i >= 0; i--) {
			ArrayList al = (ArrayList) queryResults.get(i);
			ArrayList<Integer> indexes = new ArrayList<Integer>();
			indexes.addAll(hs.values());
			for (int ind : indexes) {
				System.out.print(String.format("%-20s",al.get(ind)));
			}
			System.out.println();
		}
	}

}

package programmingAssignment2;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class LeafPage {
	int typeOfPage;
	static int numberOfRecords;
	String startOfContent;
	String rightPagePtr;
	int noOfPages;

	LeafPage() {
		typeOfPage = 13;
		numberOfRecords = 0;
		startOfContent = "";
		rightPagePtr = "FFFFFFFF";
		noOfPages = 1;
	}

	public void leafPageHeader(RandomAccessFile tableInsFile) {
		try {
			tableInsFile.seek(((noOfPages - 1) * 512) + 0);
			tableInsFile.write(typeOfPage);
			tableInsFile.seek(((noOfPages - 1) * 512) + 1);
			tableInsFile.write(numberOfRecords);
			tableInsFile.seek(((noOfPages - 1) * 512) + 4);
			tableInsFile.write(255);
			tableInsFile.seek(((noOfPages - 1) * 512) + 5);
			tableInsFile.write(255);
			tableInsFile.seek(((noOfPages - 1) * 512) + 6);
			tableInsFile.write(255);
			tableInsFile.seek(((noOfPages - 1) * 512) + 7);
			tableInsFile.write(255);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void LeafPageRecord(RandomAccessFile tableInsFile, LinkedHashMap<String, Integer> colDet, int recSize) {

		try {
			tableInsFile.seek(((noOfPages - 1) * 512) + 1);
			tableInsFile.write(numberOfRecords);
			tableInsFile.seek(((noOfPages - 1) * 512) + 2);
			int recPostStrt = ((noOfPages - 1) * 512) + 512 - (numberOfRecords * recSize);
			tableInsFile.writeShort(recPostStrt);
			tableInsFile.seek(((noOfPages - 1) * 512) + 8 + ((numberOfRecords - 1) * 2));
			tableInsFile.writeShort(recPostStrt);

			int payLoad = 0;

			tableInsFile.seek(recPostStrt + 2);
			tableInsFile.writeInt(numberOfRecords);// rowid
			tableInsFile.seek(recPostStrt + 6);
			tableInsFile.write(colDet.size());// num of cols

			Iterator colDetIt = colDet.entrySet().iterator();
			int strtPosition = recPostStrt + 7;
			while (colDetIt.hasNext()) {
				HashMap.Entry pair = (HashMap.Entry) colDetIt.next();
				payLoad += (int) pair.getValue() + 1;
				tableInsFile.seek(strtPosition);
				Class getClassStr = pair.getKey().getClass();
				if (getClassStr.toGenericString().contains("String")) {
					int size = 12 + (int) pair.getValue();
					tableInsFile.write(size);
					tableInsFile.seek(++strtPosition);
					tableInsFile.write(((String) pair.getKey()).getBytes());
					strtPosition += (int) pair.getValue();
				} else {
					tableInsFile.write(9);
					tableInsFile.seek(++strtPosition);
					tableInsFile.writeInt(Integer.parseInt(pair.getKey().toString()));
					strtPosition += 4;
				}
			}
			tableInsFile.seek(recPostStrt);
			tableInsFile.writeShort(payLoad);
		} catch (IOException e) {
			e.printStackTrace();
		}
		checkSpaceOnFile(tableInsFile, recSize);

	}

	public void checkSpaceOnFile(RandomAccessFile insTable, int cellSize) {
		try {
			insTable.seek(((noOfPages - 1) * 512) + 2);
			short lastRecPos = insTable.readShort();
			int firstRecPos = ((noOfPages - 1) * 512) + (8 + ((numberOfRecords - 1) * 2));
			int space = lastRecPos - firstRecPos;
			if (space < cellSize) {
				insTable.setLength(insTable.length() + 512);
				noOfPages = (int) (insTable.length() / 512);
				numberOfRecords = 0;
				leafPageHeader(insTable);

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void checkNumOfRecords(RandomAccessFile tableInsFile) {
		try {
			noOfPages = (int) (tableInsFile.length() / 512);
			tableInsFile.seek(((noOfPages - 1) * 512) + 1);
			numberOfRecords = tableInsFile.read();
			numberOfRecords++;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ArrayList processSelect(RandomAccessFile dbColsTable, String tableName) {

		ArrayList resultSet = new ArrayList();
		int pageOtherThanLast = 0;
		try {
			noOfPages = (int) (dbColsTable.length() / 512);
			dbColsTable.seek(((noOfPages - 1) * 512) + 1);
			numberOfRecords = dbColsTable.read();

			if (numberOfRecords == 0) {
				noOfPages--;

				dbColsTable.seek(((noOfPages - 1) * 512) + 1);
				numberOfRecords = dbColsTable.read();
			}
			pageOtherThanLast = noOfPages - 1;
			int seekPos = ((noOfPages - 1) * 512) + (8);
			dbColsTable.seek(seekPos);
			short pos1 = dbColsTable.readShort();
			int cellSize = noOfPages * 512 - pos1;
			seekPos = ((noOfPages - 1) * 512) + (8 + ((numberOfRecords - 1) * 2));
			dbColsTable.seek(seekPos);
			pos1 = dbColsTable.readShort();
			pos1 += 7;
			while (noOfPages >= -1) {
				if (noOfPages <= pageOtherThanLast) {
					dbColsTable.seek(((noOfPages - 1) * 512) + 1);
					int tempCheck = numberOfRecords;
					int noOfRex = dbColsTable.read();
					if (tempCheck == 0) {
						numberOfRecords = noOfRex;
					}
					pos1 = (short) ((noOfPages) * 512 - (numberOfRecords * cellSize));
					pos1 += 7;
				}

				int innPos = pos1;
				int someInt = 7;
				ArrayList rowVal = new ArrayList();
				while (someInt <= cellSize) {

					dbColsTable.seek(pos1);
					int colSize = dbColsTable.read();
					if (pos1 % 512 == 0) {
						break;
					}
					if (colSize > 12) {
						dbColsTable.seek(pos1 + 1);
						byte[] readStr = new byte[20];

						dbColsTable.read(readStr, 0, colSize - 12);
						String readByteStr = new String(readStr, "UTF-8");
						rowVal.add(readByteStr.trim());
						someInt += 1 + (colSize - 12);
						pos1 += ((colSize - 12) + 1);
					} else if (colSize == 9) {
						dbColsTable.seek(pos1 + 1);
						int intVal = dbColsTable.readInt();
						rowVal.add(intVal);
						someInt += 5;
						pos1 += 5;
					} else {
						break;
					}
				}

				if (!tableName.equals("")) {
					if (rowVal.contains(tableName)) {
						resultSet.add(rowVal);
					}
				}else{
					resultSet.add(rowVal);
				}

				pos1 = (short) (innPos + cellSize);
				numberOfRecords--;
				if (numberOfRecords == 0) {
					noOfPages--;
				}
			}

		} catch (IOException e) {
			try {
				dbColsTable.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		return resultSet;
	}

}

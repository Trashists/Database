#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <cstring>
#include <stdio.h>
#include "../storageEngine/database.cpp"

using namespace std;

class ResultSet {
public:
    string getName() {
        return result_set_name;
    }

    void setName(string name) {
        result_set_name = name;
    }

    void setColumnDefs(ColumnDefs columndefs) {
        column_defs = columndefs;
    }

    ColumnDefs getColumnDefs() {
        return column_defs;
    }

    int getRowCount() {
        return rows.size();
    }

    void setRowCount(int count) {
        row_count = count;
    }

    void insertRowIntoIndex(Row row, int index) {
        rows[index] = row;
    }

    void setRowAtIndex(Row row, int index) {
        rows[index] = row;
    }

    void insertRow(Row row) {
        if (row.getColumnDefs().isDifferentColumnDefs(column_defs)) {
            throw std::runtime_error("Row's structure doesn't fit result set " + result_set_name + "'s structure");
        }
        else {
            rows[row_count] = row;
            row_count += 1;
        }
    }

    Row readRow(int index) {
        if (index < 0 || index >= row_count) {
            throw runtime_error("invalid access to result_set");
        }
        else {
            return rows[index];
        }
    }

    void to_csv(string filename = "") {
        if (filename == "") filename = result_set_name + ".csv";
        else filename += ".csv";
        ofstream outfile(filename);

        for (int i = 0; i < column_defs.getColumnCount(); i++) {
            outfile << column_defs.getColumnName(i) << ",";
        }
        outfile << '\n';

        for (int j = 0; j < row_count; j++)
        {
            for (int i = 0; i < column_defs.getColumnCount(); i++){
                DBValue value = rows[j].getValueByColumnName(column_defs.getColumnName(i));
                if (value.getType() == DBType::INT) {
                    outfile << value.getIntValue() << ",";
                }
                else if (value.getType() == DBType::FLOAT){
                    outfile << value.getFloatValue() << ",";
                }
                else {
                    outfile << value.getStringValue() << ",";
                }
            }
            outfile << '\n';
        }
        outfile.close();
    }

private:
    int row_count;
    string result_set_name;
    ColumnDefs column_defs;
    map<int, Row> rows;
};

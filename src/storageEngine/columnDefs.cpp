#include <cstring>
#include <vector>
#include <map>
#include <iostream>
#include <stdio.h>
#include "../../include/zeroTrailing.h"
#include "storageEngine.cpp"

using namespace std;

class DBType {
public:
    enum Type {INT, FLOAT, STRING};
};

class DBValue {
public:
    DBValue() {}
    
    //Constructor for each type of value:
    DBValue(int value) {
        type_ = DBType :: INT;
        int_value = value;
    }

    DBValue(float value) {
        type_ = DBType :: FLOAT;
        float_value = value;
    }

    DBValue(const string value) {
        type_ = DBType :: STRING;
        str_value = value;
    }

    DBType :: Type getType() {
        return type_;
    }

    int getIntValue() {
        return int_value;
    }

    float getFloatValue() {
        return float_value;
    }

    string getStringValue() {
        return str_value;
    }

    int getWidth() {
        if (type_ == DBType :: INT) {
            return sizeof(int);
        }
        if (type_ == DBType :: FLOAT) {
            return sizeof(float);
        }
        if (type_ == DBType :: STRING) {
            return 30;
        }
        else {
            return 0;
        }
    }

    void setInt(int num) {
        int_value = num;
    }

    void setFloat(float f) {
        float_value = f;
    }

    void setString(string str) {
        str_value = str;
    }

    void setType(DBType :: Type type) {
        type_ = type;
    }


private:
    DBType :: Type type_; //type
    int int_value; // Integer value
    float float_value; // Float value
    string str_value; // String value
};

class ColumnDef {
public:
    ColumnDef() {}
    ColumnDef(string name, DBType::Type type, int width)
        {
            name_ = name;
            type_ = type;
            width_ = width;
        }

    string getName() {
        return name_;
    }

    void setName(string column_name) {
        name_ = column_name;
    }

    DBType::Type getType() {
        return type_;
    }

    void setType(DBType::Type column_type) {
        type_ = column_type;
    }

    int getWidth() {
        if (getType() == DBType :: INT) {
            return sizeof(int);
        }
        if (getType() == DBType :: FLOAT) {
            return sizeof(float);
        }
        if (getType() == DBType :: STRING) {
            return 30;
        };
        return 0;
    }

    void setWidth(int column_width) {
        width_ = column_width;
    }

    bool isDifferentColumnDef(ColumnDef other) {
        if (name_ != other.getName() && type_ != other.getType()) {
            return true;
        }
        else {
            return false;
        }
    }

private:
    string name_;
    DBType :: Type type_;
    int width_;
};

class ColumnDefs {
public:
    map<string, ColumnDef> columns_;
    map<int, string> columnsName;
    
    ColumnDefs() {}

    void addColumn(ColumnDef column) {
        int size = columns_.size();
        columnsName[size] = column.getName();
        columns_[column.getName()] = column;
    }

    int getColumnCount()  {
        return columns_.size();
    }

    string getColumnName(int i) {
        return columnsName[i];
    }

    ColumnDef getColumnDef(string ColumnName) {
        if (!columns_.count(ColumnName)) {
            throw out_of_range("Column index out of range");
        }
        return columns_[ColumnName];
    }

    int getRowSize() {
        int rowSize_ = 0;
            for (auto& column : columns_) {
                rowSize_ += column.second.getWidth();
            }
        return rowSize_;
    }

    bool isDifferentColumnDefs(ColumnDefs colDefs) {
        bool different = true;
        for (int i = 0; i < columns_.size(); i++) {
            ColumnDef current_ColumnDef = colDefs.columns_[columnsName[i]];
            if (columns_[columnsName[i]].isDifferentColumnDef(current_ColumnDef)) {
                different = different && true; 
            }
            else {
                different = different && false;
            }
        }
        return different;
    }
};


class Row {
public:
    Row() {}
    Row(ColumnDefs columnDefs)
    {
        columnDefs_ = columnDefs;
    }

    ColumnDefs getColumnDefs() const {
        return columnDefs_;
    }

    void setColumnDefs(ColumnDefs col_defs) {
        //set the columnDefs
        columnDefs_ = col_defs;
        //DBValue init_value;
        //set up the maps of the row
        /*for (int i = 0; i < col_defs.getColumnCount(); i++) {
            this -> setValueByColumnName(col_defs.columnsName[i], init_value);
        }*/
    }

    void setValueByColumnName(string name, DBValue value) {
        //Check if there exists a column with name name in Row
        if (values_.find(name) != values_.end()) {
            values_[name] = value;
        }
        else {
            int size = values_.size();
            values_name[size] = name;
            values_[name] = value;
        }
    }

    DBValue getValueByColumnName(string name) {
        //Check if there exists a column with name name in the row
        if (values_.find(name) != values_.end()) {
            return values_[name];
        }
        else {
            DBValue ret_val;
            cout << "No such column name " << name << " in row" << endl;
            return ret_val;
        }
    }
    
    //encode the Row (which is a list of DBValue) into the byte buffer (binary array)
    char* encode() {
        ColumnDefs colDefs = getColumnDefs();
        int rowBufferSize = getColumnDefs().getRowSize();
        int rowLength = values_.size();
        int offset = 0;

        //allocate memory for rowBuffer_
        rowBuffer_ = new char[columnDefs_.getRowSize()];

        //clean the buffer
        memset(rowBuffer_, 0, rowBufferSize);

        //encoding process
        for (int i = 0; i < rowLength; i++) {
            if (values_[values_name[i]].getType() == DBType :: INT) {
                //Convert integer value to byte array and copy it to the rơBuffer
                int int_value = values_[values_name[i]].getIntValue();
                memcpy(rowBuffer_ + offset, &int_value, sizeof(int));
                offset += sizeof(int);
            }
            if (values_[values_name[i]].getType() == DBType :: FLOAT) {
                float float_value = values_[values_name[i]].getFloatValue();
                memcpy(rowBuffer_ + offset, &float_value, sizeof(float));
                offset += sizeof(float);
            }

            if (values_[values_name[i]].getType() == DBType :: STRING) {
                string str_value = values_[values_name[i]].getStringValue();
                int str_length = str_value.size();
                char* char_str_value = &str_value[0];
                memcpy(rowBuffer_ + offset, char_str_value, str_length);
                offset += str_length;
                if (str_length < values_[values_name[i]].getWidth()) {
                    memset(rowBuffer_ + offset + str_length, 0, values_[values_name[i]].getWidth() - str_length);
                    offset += values_[values_name[i]].getWidth() - str_length;
                }
            }
        }
        return rowBuffer_;
    }

    /*decode from byte array (binary array) to build back the row (the list of DBValue)
    takes the buffer as input and build back by the columndefs of the row */
    void decode(char* buffer) {
        DBValue value;
        int row_length = columnDefs_.columns_.size();
        int offset = 0;
        for (int i = 0; i < row_length; i++) {
            if (columnDefs_.columns_[columnDefs_.columnsName[i]].getType() == DBType :: INT) {
                int num;
                memcpy(&num, buffer + offset, sizeof(int)); 
                value.setInt(num);
                this -> setValueByColumnName(columnDefs_.columnsName[i], value);
            }
            if (columnDefs_.columns_[columnDefs_.columnsName[i]].getType() == DBType :: FLOAT) {
                float f;
                memcpy(&f, buffer + offset, sizeof(float));
                value.setFloat(f);
                this -> setValueByColumnName(columnDefs_.columnsName[i], value);
            }
            if (columnDefs_.columns_[columnDefs_.columnsName[i]].getType() == DBType :: STRING) {
                string str(buffer + offset, 30);
                string str_ = removeTrailingZeros(str);
                value.setString(str_);
                this -> setValueByColumnName(columnDefs_.columnsName[i], value);  
            }  
            offset += columnDefs_.columns_[columnDefs_.columnsName[i]].getWidth();
        }
    }

private:
    ColumnDefs columnDefs_;
    //
    map<string, DBValue> values_;
    //
    map<int, string> values_name;
    map<string, ColumnDef> columns = columnDefs_.columns_;
    char* rowBuffer_;
};

class Table {
public:
    virtual void insertRow(Row row) = 0;
    virtual Row readRow(int index) = 0;
    virtual int getRowCount() = 0;
    virtual void setRowCount(int n) = 0;
};

//Derived class of Table, class of User defined table
class UserTable : public Table {
public:
    UserTable()
    {}
    
    UserTable(ColumnDefs col_Defs) {
        columnDefs_ = col_Defs;
    }

    void setName(string name) {
        table_name = name;
    }

    string getName() {
        return table_name;
    }

    void setColumnDefs(ColumnDefs columndefs) {
        columnDefs_ = columndefs;
    }

    ColumnDefs getColumnDefs() {
        return columnDefs_;
    }

    //function to insert a row to the end of the table
    void insertRow(Row row) override {
        //handle the case where row has a different columndefs than Table
        if (row.getColumnDefs().isDifferentColumnDefs(columnDefs_)) {
            cout << "Row's structure doesn't fit Table " + table_name + "'s structure" << endl;
            throw std::runtime_error("Row's structure doesn't fit Table's " + table_name + "'s structure");
        }
        else {
            //encode Row into Byte Buffer
            char* buffer = row.encode();
            //write the byteBuffer to files
            setDataSegment().write(row_count, buffer);
            //Update row_count
            row_count += 1;
        }
    }

    //function to read a row at a specific index of the table
    Row readRow(int index) override {
        Row row;
        row.setColumnDefs(columnDefs_);
        char* buffer = setDataSegment().read(index);
        row.decode(buffer);
        return row;
    }

    void setRowCount(int n) override {
        row_count = n;
    }

    int getRowCount() override {
        return row_count;
    }

    void setDatabaseName(string name) {
        DatabaseName = name;
    }

    //set and get the corresponding data segment (the data processing part of a table)
    Segment setDataSegment() {
        Segment segment;
        map<int, int> columns_width;
        int i = 0;
        int current_extent = 0;
        int size = columnDefs_.columns_.size();
        int table_size = columnDefs_.getRowSize();
        for (int i = 0; i < size; i++) {
            columns_width[i] = columnDefs_.columns_[columnDefs_.columnsName[i]].getWidth();
        }
        segment.setSegmentSize(table_size);
        segment.initialise_Extent(columns_width);
        for(int i = 0; i < size; i++) {
            segment.setExtentsFile(current_extent, DatabaseName + "/" + table_name + "-" + columnDefs_.columnsName[i] + ".bin");
            current_extent += 1;
        }
        return segment;
    }

private:
    int row_count;
    string table_name;
    ColumnDefs columnDefs_;
    Segment DataSegment;
    Segment Index;
    string DatabaseName;
};

//Derived class of Table, class of system's table
class SystemTable : public Table {
public:
    SystemTable() {}
    SystemTable(ColumnDefs col_Defs) {
        columnDefs_ = col_Defs;
    }

    string getName() {
        return table_name;
    }

    void setName(string name) {
        table_name = name;
    }

    void setColumnDefs(ColumnDefs columndefs) {
        columnDefs_ = columndefs;
    }

    ColumnDefs getColumnDefs() {
        return columnDefs_;
    }

    void insertRow(Row row) override {
        //handle the case where row has a different columndefs than Table
        if (row.getColumnDefs().isDifferentColumnDefs(columnDefs_)) {
            cout << "Row's structure doesn't fit Table's structure" << endl;
            throw std::runtime_error("Row's structure doesn't fit Table " + table_name + " structure");
        }
        else {
            //encode Row into Byte Buffer
            char* buffer = row.encode();
            //write the byteBuffer to files
            setDataSegment().write(row_count, buffer);
            //Update row_count
            row_count += 1;
        }
    }

    void updateRow(Row row, int index) {
        //handle the case where row has a different columndefs than Table
        if (row.getColumnDefs().isDifferentColumnDefs(columnDefs_)) {
            cout << "Row's structure doesn't fit Table's " + table_name + " structure" << endl;
            throw std::runtime_error("Row's structure doesn't fit Table " + table_name + "'s structure");
        }
        else {
            //encode Row into Byte Buffer
            char* buffer = row.encode();
            //write the byteBuffer to files
            setDataSegment().write(index, buffer);
        }
    }

    Row readRow(int index) override {
        Row row;
        row.setColumnDefs(columnDefs_);
        char* buffer = setDataSegment().read(index);
        row.decode(buffer);
        return row;
    } //string khi được read sẽ ra nguyên cái buffer khác

    void setRowCount(int n) override {
        row_count = n;
    }

    int getRowCount() override {
        return row_count;
    }

    void setDatabaseName(string name) {
        DatabaseName = name;
    }

    //set and get the corresponding data segment (the data processing part of a table)
    Segment setDataSegment() {
        Segment segment;
        map<int, int> columns_width;
        int current_extent = 0;
        int size = columnDefs_.columns_.size();
        int table_size = columnDefs_.getRowSize();
        for (int i = 0; i < size; i++) {
            string column_name = columnDefs_.columnsName[i];
            columns_width[i] = columnDefs_.columns_[columnDefs_.columnsName[i]].getWidth();
        }
        segment.setSegmentSize(table_size);
        segment.initialise_Extent(columns_width);
        for(int i = 0; i < size; i++) {
            segment.setExtentsFile(current_extent, DatabaseName + "/" + table_name + "-" + columnDefs_.columnsName[i] + ".bin");
            current_extent += 1;
        }
        return segment;
    }

private:
    int row_count;
    string table_name;
    ColumnDefs columnDefs_;
    Segment DataSegment;
    Segment Index;
    string DatabaseName;
};
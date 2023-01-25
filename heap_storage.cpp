#include "heap_storage.h"
#include "db_cxx.h"
#include "storage_engine.h"
#include <cassert>
#include <vector>
#include <cstring>
#include <iostream>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
using namespace std;
//bool test_heap_storage() {return true;}
DbEnv* _DB_ENV;

/* FIXME FIXME FIXME */
typedef u_int16_t u16;
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}
void SlottedPage::put(RecordID record_id, const Dbt &data){
	u16 size = get_n(4*record_id);
	u16 location= get_n(4*record_id+2);
	u16 newsize = (u16)data.get_size();
	if(newsize>size){
		 if(!this->has_room(newsize-size)){
		 	cout<<" no space for new page"<<endl;
		 }
		 else{
		 	memcpy(this->address(location-newsize-size),data.get_data(),newsize);
            this->slide(location,location-newsize-size);
		 }
	}
	else{
		memcpy(this->address(location),data.get_data(),newsize);
		this->slide(location+newsize,location+size);
	}
	get_header(size, location, record_id);
	put_header(record_id, newsize, location);
}
void SlottedPage::del(RecordID record_id) {
	u16 size;
	u16 location;
	get_header(size, location, record_id);
	put_header(record_id, 0, 0);
	slide(location, location + size);
}
void SlottedPage::slide(u_int16_t start, u_int16_t end){
	 u16 shift = end -start;
	 if(shift==0) return;
	 // slide data
	memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_free + 1), start);
	 //fixup headers
	u16 size;
	u16 location;
	RecordIDs* idset = this->ids();
	for(RecordID id:*idset){
		get_header(size, location,id);
		if (location <= start) {
			location += shift;
			put_header(id, size, location);
		}
	}
	this->end_free += shift;
	this->put_header();
	delete idset; 
}

bool SlottedPage::has_room(u_int16_t size) {
	u_int16_t available=this->end_free - (this->num_records + 2) * 4;
	return (size <= available);
}
RecordIDs* SlottedPage::ids(void){
	u16 size,loc;
	RecordIDs* idsets = new RecordIDs;

	for (u16 i = 1; i <= this->num_records; i++) {
		get_header(size, loc, i);
		if (loc > 0) {
			idsets->push_back(i);
		}
	}
	return idsets;
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}
void SlottedPage::get_header(u16& size, u16& loc, RecordID id) {
        size = get_n(4*id);
        loc =  get_n(4*id+2);
   
}

Dbt* SlottedPage::get(RecordID id) {
    u16 size;
    u16 loc;
   size=get_n(4*id);
   loc =get_n(4*id+2);
   get_header(size, loc, id);
    if (loc == 0) {
        return nullptr;
    }
    return new Dbt(this->address(loc), size);
}
/*** HeapFile implement***/
void HeapFile::db_open(uint openflags) {
	if (!this->closed) {
		return;
	}
	this->db.set_re_len(DbBlock::BLOCK_SZ);
	const char* devpath = nullptr;
	_DB_ENV->get_home(&devpath);
	this->dbfilename = "./" + this->name + ".db";
	this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, openflags, 0644);
	DB_BTREE_STAT *stat;
	this->db.stat(nullptr, &stat, DB_FAST_STAT);
	this->last = openflags ? 0 : stat->bt_ndata;
	this->closed = false;
}
//Create physical File
void HeapFile::create(void) {
	db_open(DB_CREATE|DB_EXCL); 
	SlottedPage* block = get_new();
	this->put(block);
}
//Delete file
void HeapFile::drop(void) {
	close();
	Db db(_DB_ENV, 0);
	db.remove(this->dbfilename.c_str(), nullptr, 0);
}

//Open file
void HeapFile::open(void) {
	db_open();
}
//Close file
void HeapFile::close(void) {
	db.close(0);
	closed = true;

}
SlottedPage* HeapFile::get(BlockID block_id) {
	Dbt key(&block_id, sizeof(block_id));
	Dbt data;
	this->db.get(nullptr, &key, &data, 0);
	return new SlottedPage(data, block_id, false);
}
// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

/*** HeapTable implement***/
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
: DbRelation(table_name, column_names, column_attributes), file(table_name){}

void HeapTable::create() {
	file.create();
}
void HeapTable::create_if_not_exists() {
	try {
		open();
	}
	catch (DbException& e) {
		create();
	}
}
//Excecute: DROP TABLE <table_name>
void HeapTable::drop() {
	file.drop();
}

//Open existing table. Enables: insert, update, delete, select, project
void HeapTable::open() {
	file.open();
}
void HeapTable::close(){
	file.close();
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}
// test function -- returns true if all tests pass
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;
/*
    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();
   */ 

    return true;
}


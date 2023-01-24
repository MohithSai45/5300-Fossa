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
bool test_heap_storage() {return true;}

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
		 	count<<" no space for new page"<<endl
		 }
		 else{
		 	this->slide(location,location-newsize-size)
		 	memcpy(this->address(location-newsize-size),data.get_data,newsize);
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
	get_header(size, loc, record_id);
	put_header(record_id, 0, 0);
	slide(loc, loc + size);
}
void slide(u_int16_t start, u_int16_t end){
	 u16 shift = end =start
	 if(shift==0) return;
	 // slide data
	memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_free + 1), start);
	 //fixup headers
	u16 size;
	u16 loc;
	RecordIDs* idset = this->ids();
	for(RecordID id:*idset){
		get_header(size, loc,id);
		if (loc <= start) {
			loc += shift;
			put_header(id, size, loc);
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
	u16 size,loc
	RecordIDs* idsets = new RecordIDs;

	for (u16 i = 1; i <= this->num_records; i++) {
		get_header(size, loc, i);
		if (loc != 0) {
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
void SlottedPage::get_header(RecordID id, u16 size, u16 loc) {
        size = get_n(4*id);
        loc =  get_n(4*id+2);
   
}

void SlottedPage::get(RecordID id, u16 size, u16 loc) {
   size=get_n(4*id);
   loc =get_n(4*id+2)
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


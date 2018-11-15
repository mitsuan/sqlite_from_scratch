#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<stdbool.h>
#include<stdint.h>

#include<errno.h>
#include<fcntl.h>
#include<unistd.h>

struct InputBuffer_t
{
	char* buffer;
	ssize_t buffer_length;
	ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;


enum MetaCommandResult_t
{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
};
typedef enum MetaCommandResult_t MetaCommandResult;

enum PrepareResult_t
{
	PREPARE_SUCCESS,
	PREPARE_STRING_TOO_LONG,
	PREPARE_NEGATIVE_ID,
	PREPARE_SYNTAX_ERROR,
	PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareResult_t PrepareResult;

enum StatementType_t
{
	STATEMENT_INSERT,
	STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

enum ExecuteResult_t
{
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
};
typedef enum ExecuteResult_t ExecuteResult;

enum NodeType_t
{
	NODE_INTERNAL,
	NODE_LEAF
}; 
typedef enum NodeType_t NodeType;

//const uint32_t COLUMN_USERNAME_SIZE 32;
//const uint32_t COLUMN_EMAIL_SIZE  255;
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
struct Row_t
{
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1];
	char email[COLUMN_EMAIL_SIZE + 1];
};
typedef struct Row_t Row;

#define size_of_attribute(Struct,Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row,id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row,username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row,email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET+ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET+USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE+USERNAME_SIZE+EMAIL_SIZE;

const uint32_t PAGE_SIZE=4096;
//const uint32_t TABLE_MAX_PAGES 100;
#define TABLE_MAX_PAGES 100
// const uint32_t ROWS_PER_PAGE=PAGE_SIZE/ROW_SIZE;
// const uint32_t TABLE_MAX_ROWS=ROWS_PER_PAGE * TABLE_MAX_PAGES;


/*
*Common Node Header Layout
*/
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_SIZE + IS_ROOT_OFFSET;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE; 

/*
* Leaf Node Header Layout
*/
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;


/*
*	Leaf Node Body layout
*/
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE +LEAF_NODE_KEY_OFFSET;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE + LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;


struct Statement_t
{
	StatementType type;
	Row row_to_insert;  //only used by insert statement
};
typedef struct Statement_t Statement;

struct Pager_t
{
	int file_descriptor;
	uint32_t file_length;
	uint32_t num_pages;		//+
	void* pages[TABLE_MAX_PAGES];
	
};
typedef struct Pager_t Pager;

struct Table_t
{
	Pager* pager;
//	void* pages[TABLE_MAX_PAGES];
	// uint32_t num_rows;
	uint32_t root_page_num;

};
typedef struct Table_t Table;

typedef struct Cursor_t
{
	Table* table;
	// uint32_t row_num;
	uint32_t page_num;
	uint32_t cell_num;
	bool end_of_table;
		
} Cursor;



InputBuffer* new_input_buffer()
{
	InputBuffer* input_buffer=malloc(sizeof(InputBuffer));

	input_buffer->buffer=NULL;
	input_buffer->buffer_length=0;
	input_buffer->input_length=0;

	return input_buffer;
}

void print_prompt()
{
	printf("db> ");
}

/*
*	PRINTING CONSTANTS
*/

void print_constants()
{
	printf("ROW_SIZE: %d\n",ROW_SIZE);
	printf("COMMON_NODE_HEADER_SIZE: %d\n",COMMON_NODE_HEADER_SIZE);
	printf("LEAF_NODE_HEADER_SIZE: %d\n",LEAF_NODE_HEADER_SIZE);
	printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
	printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n",LEAF_NODE_SPACE_FOR_CELLS);
	printf("LEAF_NODE_MAX_CELLS: %d\n",LEAF_NODE_MAX_CELLS);
}

/*
*	VISUALIZING TREE
*/
void print_leaf_node(void* node)
{
	uint32_t num_cells= *leaf_node_num_cells(node);
	printf("leaf (size %d)\n",num_cells);
	for(uint32_t i=0;i < num_cells; i++)
	{
		uint32_t key = *leaf_node_key(node, i);
		printf(" - %d : %d \n",i,key);
	}

}


void read_input(InputBuffer* input_buffer)
{
	ssize_t bytes_read=getline(&(input_buffer->buffer),&(input_buffer->buffer_length),stdin);

	if(bytes_read<=0)
	{
		printf("Error reading input\n");
		exit(EXIT_FAILURE);
	}

	//Ignore trailing newline
	input_buffer->buffer[bytes_read-1]=0;
	input_buffer->input_length=bytes_read-1;

}

void db_close(Table*);

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table)
{
	if(strcmp(input_buffer->buffer,".exit")==0)
	{
		db_close(table);
		exit(EXIT_SUCCESS);
	}
	else if(strcmp(input_buffer->buffer,".constants")==0)
	{
		printf("Constants:\n");
		print_constants();
		return META_COMMAND_SUCCESS;
	}
	else if(strcmp(input_buffer->buffer,".btree")==0)
	{
		printf("Tree:\n");
		print_leaf_node(get_page(table->pager,0))
		return META_COMMAND_SUCCESS;
	}
	else
		return META_COMMAND_UNRECOGNIZED_COMMAND;
}


PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement)
{
	statement->type= STATEMENT_INSERT;
	
	char* keyword = strtok(input_buffer->buffer, " ");
	char* id_string = strtok(NULL," ");
	char* username = strtok(NULL," ");
	char* email = strtok(NULL," ");

	if(id_string==NULL||  username==NULL || email==NULL)
		return PREPARE_SYNTAX_ERROR;
		
	int id = atoi(id_string);
	if(id<0)
	{
		return PREPARE_NEGATIVE_ID;
	}
	
	if(strlen(username) > COLUMN_USERNAME_SIZE)
	{
		return PREPARE_STRING_TOO_LONG;
	}
	
	if(strlen(email) > COLUMN_EMAIL_SIZE)
	{
		return PREPARE_STRING_TOO_LONG;
	} 
	
	statement->row_to_insert.id=id;
	strcpy(statement->row_to_insert.username,username);
	strcpy(statement->row_to_insert.email,email);
	
	return PREPARE_SUCCESS;

}


PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement)
{
	if(strncmp(input_buffer->buffer,"insert",6)==0)
	{
	
		return prepare_insert(input_buffer,statement);
/*		statement->type=STATEMENT_INSERT;
		
		int args_assigned= sscanf(input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),&(statement->row_to_insert.username),&(statement->row_to_insert.email));
		
		if(args_assigned<3)
			return PREPARE_SYNTAX_ERROR;
			
		return PREPARE_SUCCESS;*/
		
	}
	if(strcmp(input_buffer->buffer,"select")==0)
	{
		statement->type=STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	
	return PREPARE_UNRECOGNIZED_STATEMENT;
	
	
}

void print_row(Row* row)
{
	printf("(%d, %s, %s)\n",row->id,row->username,row->email);
}

void serialize_row(Row* source,void* destination)
{
	memcpy(destination+ID_OFFSET, &(source->id),ID_SIZE);
	memcpy(destination+USERNAME_OFFSET, &(source->username),USERNAME_SIZE);
	memcpy(destination+EMAIL_OFFSET, &(source->email),EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination)
{
	memcpy(&(destination->id),source+ID_OFFSET,ID_SIZE);
	memcpy(&(destination->username),source+USERNAME_OFFSET,USERNAME_SIZE);
	memcpy(&(destination->email),source+EMAIL_OFFSET,EMAIL_SIZE);
}

void* get_page(Pager* pager, uint32_t page_num)
{
	if(page_num > TABLE_MAX_PAGES)
	{
		printf("Tried to fetch page number out of bounds. %d > %d\n",page_num,TABLE_MAX_PAGES);
		exit(EXIT_FAILURE);
	}
	
	if(pager->pages[page_num]==NULL)
	{
		//Cache miss. Allocate memory and load from file.
		void* page = malloc(PAGE_SIZE);
		uint32_t num_pages = pager->file_length/PAGE_SIZE;
		
		//debug
		//printf("num_pages: %d\n",num_pages);
		
		//We might save a partial page at the end of the file.
		if(pager->file_length%PAGE_SIZE)
		{
			//debug 
			//printf("has partial page. num pages: %d\n",num_pages+1);
			num_pages+=1;
			
		}
		
		if(page_num<num_pages)
		{
			lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
			ssize_t bytes_read=read(pager->file_descriptor,page,PAGE_SIZE);
			if(bytes_read==-1)
			{
				printf("Error reading file: %d\n",errno);
				exit(EXIT_FAILURE);
			}
		}
		
		pager->pages[page_num]=page;

		
		//The above code is used for select statement
		//If rows are inserted and it exceeds the page length stored in file,
		// the below condition evalutes to true and page number is incremented.
		//If row is inserted it is inserted into new page
		//If 'select' statement is executed it reads these pages which haven't been
		// written to the file.
		if(page_num>=pager->num_pages)
		{
			pager->num_pages=page_num+1;
		}
	}
	
	return pager->pages[page_num];
	
}

/*
* Leaf Node Operations
*/
uint32_t* leaf_node_num_cells(void* node)
{
	return (char*)node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num)
{
	return (char*)node +LEAF_NODE_HEADER_SIZE+ cell_num*LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t*	leaf_node_key(void* node, uint32_t num_cell)
{
	return leaf_node_cell(node,cell_num);
}
void* leaf_node_value(void* node, uint32_t num_cell)
{
	return leaf_node_cell(node,cell_num) + LEAF_NODE_KEY_SIZE;	
}

void* initialize_leaf_node(void* node)
{
	*leaf_node_num_cells(node)=0;
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value)
{
	void* node = get_page(cursor->table->pager, cursor->page_num);

	uint32_t num_cells = *leaf_node_num_cells(node);
	if(num_cells>= LEAF_NODE_MAX_CELLS)
	{
		//Node full
		printf("Node full. Need to implement splitting of leaf node.\n");
		exit(EXIT_FAILURE);
	}

	if(cursor->cell_num < num_cells)
	{
		//Make room for new cell
		for(uint32_t i=num_cells; i > cell_num; i--)
		{
			memcpy(leaf_node_cell(node,i),leaf_node_cell(node,i-1),LEAF_NODE_CELL_SIZE);
		}
	}

	*(leaf_node_num_cells(node))+=1;
	*(leaf_node_key(node, cursor->cell_num))=key;
	serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

void cursor_advance(Cursor* cursor)
{
	// cursor->row_num+=1;
	
	// if(cursor->row_num>=cursor->table->num_rows)
	uint32_t page_num=cursor=>page_num;
	void* node = get_page(cursor->table->pager, page_num);
	cursor->cell_num+=1;
	if(cursor->cell_num>=*leaf_node_num_cells(node))
	{
		cursor->end_of_table=true;
	}
}

//void* row_slot(Table* table, uint32_t row_num)
void* cursor_value(Cursor* cursor)
{
	// uint32_t row_num=cursor->row_num;
	
	// uint32_t page_num=row_num/ROWS_PER_PAGE;
	uint32_t page_num = cursor->page_num;
/*	void* page=table->pages[page_num];
	
	if(!page)
	{
		//Allocate memory only when we try to access page
		page=table->pages[page_num]=malloc(PAGE_SIZE);
	}
	*/
	void* page=get_page(cursor->table->pager,page_num);
	
	// uint32_t row_offset=row_num%ROWS_PER_PAGE;
	// uint32_t byte_offset=row_offset*ROW_SIZE;
	
	// return page+byte_offset;	
	return leaf_node_value(page, cursor->cell_num);
}

Cursor* table_start(Table*);
Cursor* table_end(Table*);


ExecuteResult execute_insert(Statement* statement, Table* table)
{
	// if(table->num_rows >= TABLE_MAX_ROWS)
	void* node = get_page(table->pager, table->root_page_num);
	if(*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS)
	{
		return EXECUTE_TABLE_FULL;
	}
	
	Row* row_to_insert = &(statement->row_to_insert);

	Cursor* cursor = table_end(table);
	
	// serialize_row(row_to_insert,cursor_value(cursor));
	// table->num_rows+=1;

	leaf_node_insert(cursor,row_to_insert->id, row_to_insert);

	free(cursor);
	
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table)
{
	Cursor* cursor = table_start(table);

	Row row;

	// for(uint32_t i=0;i<table->num_rows;i++)
	// {
	// 	deserialize_row(row_slot(table,i),&row);
	// 	print_row(&row);
	// }

	while(!(cursor->end_of_table))
	{
		deserialize_row(cursor_value(cursor),&row);
		print_row(&row);
		cursor_advance(cursor);

	}

	return EXECUTE_SUCCESS;
}


//void execute_statement(Statement* statement)
ExecuteResult execute_statement(Statement* statement, Table* table)
{
	switch(statement->type)
	{
	
		case (STATEMENT_INSERT):
		{
			//printf("This is where we would do an insert.\n");
			//break;
			return execute_insert(statement,table);
		}
		case (STATEMENT_SELECT):
		{
			//printf("This is where we would do a select.\n");
			//break;
			return execute_select(statement,table); 
		}
	}
}


Pager* pager_open(const char* filename)
{
	int fd=open(filename, 
				O_RDWR	|		//Read / write mode
					O_CREAT,	//Create file if it doesn't exist
				S_IWUSR |		//User write permission
					S_IRUSR		//User read permission 
				);	
				
	if(fd==-1)
	{
		printf("Unable to open file.\n");
		exit(EXIT_FAILURE);
	}
	
	off_t file_length = lseek(fd,0,SEEK_END);
	
	Pager* pager = malloc(sizeof(Pager));
	pager->file_descriptor=fd;
	pager->file_length=file_length;
	pager->num_pages=file_length/PAGE_SIZE;

	if(file_length%PAGE_SIZE != 0)
	{
		printf("DB file is not a whole number of pages. Corrupted file.\n");
		exit(EXIT_FAILURE);
	}
	
	for(uint32_t i=0;i<TABLE_MAX_PAGES;i++)
	{
		pager->pages[i]=NULL;
	}
	
	return pager;
}

// void pager_flush(Pager* pager,uint32_t page_num,uint32_t size)
void pager_flush(Pager* pager,uint32_t page_num)
{
	if(pager->pages[page_num]==NULL)
	{
		printf("Tried to flush null page.\n");
		exit(EXIT_FAILURE);
	}
	
	off_t offset=lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
	
	if(offset==-1)
	{
		printf("Error seeking: %d\n",errno);
		exit(EXIT_FAILURE);
	}
	
	ssize_t bytes_written=write(pager->file_descriptor,pager->pages[page_num],PAGE_SIZE);
	
	if(bytes_written==-1)
	{
		printf("Error writing: %d.\n",errno);
		exit(EXIT_FAILURE);
	}
}

void db_close(Table* table)
{
	//debug
	//printf("closing db. num_rows: %d\n",table->num_rows);
	
	
	Pager* pager=table->pager;
	// uint32_t num_full_pages=table->num_rows/ROWS_PER_PAGE;
	
	// for(uint32_t i=0;i<num_full_pages;i++)
	for(uint32_t i=0;i<pager->num_pages;i++)
	{
		if(pager->pages[i]==NULL)
			continue;
			
		// pager_flush(pager,i,PAGE_SIZE);
		pager_flush(pager,i);
		free(pager->pages[i]);
		pager->pages[i]=NULL;
	}
	
	//There may be a partial page to write to the end of the file
	//Not needed after we switch to B-Tree
	// uint32_t num_additional_rows=table->num_rows%ROWS_PER_PAGE;
	// if(num_additional_rows>0)
	// {
	// 	uint32_t page_num=num_full_pages;
	// 	if(pager->pages[page_num]!=NULL)
	// 	{
	// 		pager_flush(pager,page_num,num_additional_rows*ROW_SIZE);
	// 		free(pager->pages[page_num]);
	// 		pager->pages[page_num]=NULL;
	// 	}
	// }
	
	int result= close(pager->file_descriptor);
	if(result==-1)
	{
		printf("Error closing db file.\n");
		exit(EXIT_FAILURE);
	}
	
	for(uint32_t i=0;i<TABLE_MAX_PAGES;i++)
	{
		void* page=pager->pages[i];
		if(page)
		{
			free(page);
			pager->pages[i]=NULL;
		}
	}
	
	free(pager);
}

//Table* new_table()
Table* db_open(const char* filename)
{
	Pager* pager = pager_open(filename);
	// uint32_t num_rows= pager->file_length/ROW_SIZE;
	
	Table* table=malloc(sizeof(Table));
//	table->num_rows=0;
	table->pager=pager;
	// table->num_rows=num_rows;

	if(pager->num_pages==0)
	{
		//New database file. Initialize page 0 as leaf node.
		void* root_node = get_page(pager,0);
		initialize_leaf_node(root_node);

	}
	
	return table; 
} 

Cursor* table_start(Table* table)
{
	Cursor* cursor=malloc(sizeof(Cursor));
	cursor->table=table;
	// cursor->row_num=0;
	cursor->page_num=table->root_page_num;
	cursor->cell_num=0;
	// cursor->end_of_table=(table->num_rows==0);

	void* root_node = get_page(table->pager,table->root_page_num);
	uint32_t num_cells = *leaf_node_num_cells(root_node);
	cursor->end_of_table =num_cells==0;
	
	return cursor;
}

Cursor* table_end(Table* table)
{
	Cursor* cursor=malloc(sizeof(Cursor));
	cursor->table=table;
	// cursor->row_num=table->num_rows;
	cursor->page_num=table->root_page_num;

	void* root_node = table->root_page_num;
	uint32_t num_cells = *leaf_node_num_cells(root_node);
	cursor->cell_num = num_cells;
	cursor->end_of_table=true;
	
	return cursor;
}


int main(int argc, char* argv[])
{
/*	Simple test of Comman Line arguments
	printf("Number of args: %d\n ", argc);
	for(int i=0;i<argc;i++)
	{
		printf("%s\n",argv[i]);
	}
*/
	
//	Table* table=new_table();
	if(argc	< 2)
	{
		printf("Must supply a database filename.\n");
		exit(EXIT_FAILURE);
	}
	
	char* filename=argv[1];
	Table* table=db_open(filename);

	InputBuffer* input_buffer=new_input_buffer();

	while(true)
	{
		print_prompt();
		read_input(input_buffer);

		//debug
		//printf("input: %s\n",input_buffer->buffer);
		//Command parsing
//		if(strcmp(input_buffer->buffer,".exit") == 0)
//		{
//			exit(EXIT_SUCCESS);
//		}
//		else
//		{
//			printf("Unrecognized command '%s'. \n",input_buffer->buffer);
//		}


		if(input_buffer->buffer[0]=='.')
		{
			switch(do_meta_command(input_buffer,table))
			{
				case (META_COMMAND_SUCCESS):
					continue;
				case (META_COMMAND_UNRECOGNIZED_COMMAND):
					printf("Unrecognized command '%s'.\n",input_buffer->buffer);

			}
		}
		
		Statement statement;
		switch(prepare_statement(input_buffer,&statement))
		{
			case (PREPARE_SUCCESS):
				break;
			
			case(PREPARE_STRING_TOO_LONG):
				printf("String is too long.\n");
				continue;
				
			case(PREPARE_NEGATIVE_ID):
				printf("ID must be positive.\n");
				continue;
			case (PREPARE_SYNTAX_ERROR):
				printf("Syntax Error. Couldn't parse statement.\n");
				continue;
				
			case (PREPARE_UNRECOGNIZED_STATEMENT):
				printf("Unrecognized keyword at start of '%s'.\n",input_buffer->buffer);
				continue;
		}
		
		//execute_statement(&statement);
		//printf("Executed\n");
		switch(execute_statement(&statement,table))
		{
			case (EXECUTE_SUCCESS):
				printf("Executed.\n");
				break;
			case (EXECUTE_TABLE_FULL):
				printf("Error. Table full.\n");
				break;
		}
		
	}


}


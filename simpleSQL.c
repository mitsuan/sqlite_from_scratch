#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<stdbool.h>


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
	PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareResult_t PrepareResult;

enum StatementType_t
{
	STATEMENT_INSERT,
	STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

struct Statement_t
{
	StatementType type;
};
typedef struct Statement_t Statement;

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

MetaCommandResult do_meta_command(InputBuffer* input_buffer)
{
	if(strcmp(input_buffer->buffer,".exit")==0)
		exit(EXIT_SUCCESS);
	else
		return META_COMMAND_UNRECOGNIZED_COMMAND;
}



StatementType prepare_statement(InputBuffer* input_buffer, Statement* statement)
{
	if(strncmp(input_buffer->buffer,"insert",6)==0)
	{
		statement->type=STATEMENT_INSERT;
		return PREPARE_SUCCESS;
	}
	if(strcmp(input_buffer->buffer,"select")==0)
	{
		statement->type=STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	
	return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement)
{
	switch(statement->type)
	{
	
		case (STATEMENT_INSERT):
		{
			printf("This is where we would do an insert.\n");
			break;
		}
		case (STATEMENT_SELECT):
		{
			printf("This is where we would do a select.\n");
			break; 
		}
	}
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
			switch(do_meta_command(input_buffer))
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
			case (PREPARE_UNRECOGNIZED_STATEMENT):
				printf("Unrecognized keyword at start of '%s'.\n",input_buffer->buffer);
				continue;
		}
		
		execute_statement(&statement);
		printf("Executed\n");

	}


}

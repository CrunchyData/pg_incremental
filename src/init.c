#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "crunchy/incremental/file_list.h"
#include "utils/guc.h"


PG_MODULE_MAGIC;

/* function declarations */
void		_PG_init(void);


/*
 * _PG_init is the entry-point for the library.
 */
void
_PG_init(void)
{
	if (IsBinaryUpgrade)
		return;

	DefineCustomStringVariable("incremental.default_file_list_function",
							   gettext_noop("Default file list function to use in file "
											"list pipelines if no list_function is "
											"specified."),
							   NULL,
							   &DefaultFileListFunction,
							   DEFAULT_FILE_LIST_FUNCTION,
							   PGC_USERSET,
							   0,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("incremental.log_statement",
						   gettext_noop("Log all pipeline statements."),
						   NULL,
						   &IncrementalLogStatement,
						   true,
						   PGC_USERSET,
						   0,
						   NULL, NULL, NULL);
}

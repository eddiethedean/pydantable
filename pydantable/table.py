

class PydanTable:
    """
    A table of pydantic model rows.

    Use Cases
    ---------
    ETL with built in validation
    Extract/validate -> Transform/validate -> Load

    1. Write pydantic model for data source
    2. Etract - Read data source into PydanTable
    3. Transform - Migrations/updates/inserts
    4. Check new generated pydantic model for output data
    4. Load - Push data to SQL/CSV/NoSQL

    
    Tied to a a single pydantic model that can change with column changes (migrations)
    Migrations always result in a new PydanTable.

    Migrations
    ----------
    + Add New Column - name, type, data or default
    + Drop Column
    + Change Column type - cast values
    + Change Column name
    + Add or Drop contraint - unique
    + Joins

    Each row can be changed but is validated against the pydantic model.
    Non-migrations are done in place.

    Updates
    -------
    Update row values
    Update column values - rollback on validation error or skip

    Inserts
    -------
    Insert new row
    Bulk insert rows - rollback on validation error or skip

    Selects
    -------
    Read row my index
    Read index slice of table
    Iterate over rows

    Read
    ----
    read csv - full or chunks
    read sql table - full or chunks
    read iterable of dicts
    read iterable of tuples - with column names
    """